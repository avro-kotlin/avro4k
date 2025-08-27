package com.github.avrokotlin.avro4k.internal

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroDefault
import com.github.avrokotlin.avro4k.internal.encoder.ReorderingCompositeEncoder
import com.github.avrokotlin.avro4k.internal.schema.CHAR_LOGICAL_TYPE_NAME
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.boolean
import kotlinx.serialization.json.double
import kotlinx.serialization.json.float
import kotlinx.serialization.json.int
import kotlinx.serialization.json.long
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import java.util.WeakHashMap

internal class RecordResolver(
    private val avro: Avro,
) {
    /**
     * For a class descriptor + writerSchema, it returns a map of the field index to the schema field.
     *
     * Note: We use the descriptor in the key as we could have multiple descriptors for the same record schema, and multiple record schemas for the same descriptor.
     */
    private val fieldCache: MutableMap<Pair<SerialDescriptor, Schema>, SerializationWorkflow> = WeakHashMap()

    /**
     * Maps the class fields to the schema fields.
     * For encoding:
     * - prepares the fields to be encoded in the order they are in the writer schema
     * -
     *
     * For decoding:
     * - prepares the fields to be decoded in the order they are in the writer schema
     * - prepares the default values for the fields that are not in the writer schema
     * - prepares the fields to be skipped as they are in the writer schema but not in the class descriptor
     * - fails if a field is not optional, without a default value and not in the writer schema
     */
    fun resolveFields(
        writerSchema: Schema,
        classDescriptor: SerialDescriptor,
    ): SerializationWorkflow {
        return fieldCache.getOrPut(classDescriptor to writerSchema) {
            loadCache(classDescriptor, writerSchema)
        }
    }

    /**
     * Here the different steps to get the schema field corresponding to the serial descriptor element:
     * - class field name -> schema field name
     * - class field name -> schema field aliases
     * - class field aliases -> schema field name
     * - class field aliases -> schema field aliases
     * - if field is optional, returns null
     * - if still not found, [SerializationException] thrown
     */
    private fun loadCache(
        classDescriptor: SerialDescriptor,
        writerSchema: Schema,
    ): SerializationWorkflow {
        return SerializationWorkflow(
            computeDecodingSteps(classDescriptor, writerSchema),
            computeEncodingWorkflow(classDescriptor, writerSchema)
        )
    }

    private fun computeDecodingSteps(
        classDescriptor: SerialDescriptor,
        writerSchema: Schema,
    ): Array<DecodingStep> {
        val decodingSteps = mutableListOf<DecodingStep>()

        val writerFieldIndexToElementIndex = Array<Int?>(writerSchema.fields.size) { null }

        for (elementIndex in 0 until classDescriptor.elementsCount) {
            val writerField =
                writerSchema.findFieldMatchingWithElement(classDescriptor, elementIndex)
                    ?: continue

            if (writerFieldIndexToElementIndex[writerField.pos()] != null) {
                throw IllegalStateException(
                    "The descriptor $classDescriptor has multiple elements matching to the same writer field '$writerField'. " +
                        "This is not allowed as it would lead to ambiguous decoding."
                )
            }
            writerFieldIndexToElementIndex[writerField.pos()] = elementIndex
        }

        val visitedElements = BooleanArray(classDescriptor.elementsCount) { false }

        writerSchema.fields.forEachIndexed { writerFieldIndex, field ->
            val elementIndex = writerFieldIndexToElementIndex[writerFieldIndex]
            decodingSteps +=
                if (elementIndex != null) {
                    visitedElements[elementIndex] = true
                    DecodingStep.DeserializeWriterField(
                        elementIndex = elementIndex,
                        writerFieldIndex = writerFieldIndex,
                        schema = field.schema()
                    )
                } else {
                    DecodingStep.SkipWriterField(writerFieldIndex, field.schema())
                }
        }

        if (visitedElements.any { !it }) {
            // TODO we should not need to resolve the whole record's schema, but just extract the fields names, aliases and their order.
            //  However, the schema resolution also handle the union schema ordering based on the default value, which we also need here.
            val readerSchema = avro.schema(classDescriptor)

            // iterate over remaining elements in the class descriptor that are not in the writer schema
            // so it needs to set default values or skip them if they are optional
            visitedElements.forEachIndexed { elementIndex, visited ->
                if (visited) return@forEachIndexed

                val readerDefaultAnnotation = classDescriptor.findElementAnnotation<AvroDefault>(elementIndex)
                val readerField = readerSchema.fields[elementIndex]

                decodingSteps +=
                    if (readerDefaultAnnotation != null) {
                        DecodingStep.GetDefaultValue(
                            elementIndex = elementIndex,
                            schema = readerField.schema(),
                            defaultValue = readerDefaultAnnotation.parseValueToGenericData(readerField.schema())
                        )
                    } else if (classDescriptor.isElementOptional(elementIndex)) {
                        // There is already a kotlin default value for this element, so we can skip it.
                        // We don't want to put an implicit null/empty collection here as it would override the kotlin default.
                        DecodingStep.IgnoreOptionalElement(elementIndex)
                    } else if (avro.configuration.implicitNulls && readerField.schema().isNullable) {
                        DecodingStep.GetDefaultValue(
                            elementIndex = elementIndex,
                            schema = readerField.schema().asSchemaList().first { it.type === Schema.Type.NULL },
                            defaultValue = null
                        )
                    } else if (avro.configuration.implicitEmptyCollections && readerField.schema().isTypeOf(Schema.Type.ARRAY)) {
                        DecodingStep.GetDefaultValue(
                            elementIndex = elementIndex,
                            schema = readerField.schema().asSchemaList().first { it.type === Schema.Type.ARRAY },
                            defaultValue = emptyList<Any>()
                        )
                    } else if (avro.configuration.implicitEmptyCollections && readerField.schema().isTypeOf(Schema.Type.MAP)) {
                        DecodingStep.GetDefaultValue(
                            elementIndex = elementIndex,
                            schema = readerField.schema().asSchemaList().first { it.type === Schema.Type.MAP },
                            defaultValue = emptyMap<String, Any>()
                        )
                    } else {
                        DecodingStep.MissingElementValueFailure(elementIndex)
                    }
            }
        }
        return decodingSteps.toTypedArray()
    }

    private fun computeEncodingWorkflow(
        classDescriptor: SerialDescriptor,
        writerSchema: Schema,
    ): EncodingWorkflow {
        // Encoding steps are ordered regarding the class descriptor and not the writer schema.
        // Because kotlinx-serialization doesn't provide a way to encode non-sequentially elements.
        val missingWriterFieldsIndexes = mutableListOf<Int>()
        val visitedWriterFields = BooleanArray(writerSchema.fields.size) { false }
        val descriptorToWriterFieldIndex = IntArray(classDescriptor.elementsCount) { ReorderingCompositeEncoder.SKIP_ELEMENT_INDEX }

        var expectedNextWriterIndex = 0

        for (elementIndex in 0 until classDescriptor.elementsCount) {
            val writerField =
                writerSchema.findFieldMatchingWithElement(classDescriptor, elementIndex)
                    ?: continue

            if (visitedWriterFields[writerField.pos()]) {
                throw IllegalStateException(
                    "The descriptor $classDescriptor has multiple elements matching to the same writer field '$writerField'. " +
                        "This is not allowed as it would lead to ambiguous encoding."
                )
            }
            visitedWriterFields[writerField.pos()] = true
            descriptorToWriterFieldIndex[elementIndex] = writerField.pos()
            if (expectedNextWriterIndex != -1) {
                if (writerField.pos() != expectedNextWriterIndex) {
                    expectedNextWriterIndex = -1
                } else {
                    expectedNextWriterIndex++
                }
            }
        }

        visitedWriterFields.forEachIndexed { writerFieldIndex, visited ->
            if (!visited) {
                missingWriterFieldsIndexes += writerFieldIndex
            }
        }

        return if (missingWriterFieldsIndexes.isNotEmpty()) {
            EncodingWorkflow.MissingWriterFields(missingWriterFieldsIndexes)
        } else if (expectedNextWriterIndex == -1) {
            EncodingWorkflow.NonContiguous(descriptorToWriterFieldIndex)
        } else if (classDescriptor.elementsCount != writerSchema.fields.size) {
            EncodingWorkflow.ContiguousWithSkips(descriptorToWriterFieldIndex.map { it == ReorderingCompositeEncoder.SKIP_ELEMENT_INDEX }.toBooleanArray())
        } else {
            EncodingWorkflow.ExactMatch
        }
    }

    private fun Schema.findFieldMatchingWithElement(classDescriptor: SerialDescriptor, elementIndex: Int): Schema.Field? =
        this.findFieldNamedOrAliasedAs(avro.configuration.fieldNamingStrategy.resolve(classDescriptor, elementIndex))
            ?: classDescriptor.getElementAliases(elementIndex).firstNotNullOfOrNull { this.findFieldNamedOrAliasedAs(it) }

    private fun Schema.isTypeOf(expectedType: Schema.Type): Boolean = asSchemaList().any { it.type === expectedType }
}

internal class SerializationWorkflow(
    /**
     * Decoding steps are ordered regarding the writer schema and not the class descriptor.
     */
    val decoding: Array<DecodingStep>,
    /**
     * Encoding steps are ordered regarding the class descriptor and not the writer schema.
     */
    val encoding: EncodingWorkflow,
)

internal sealed interface EncodingWorkflow {
    /**
     * The descriptor elements exactly matches the writer schema fields as a 1-to-1 mapping.
     */
    data object ExactMatch : EncodingWorkflow

    class ContiguousWithSkips(
        val fieldsToSkip: BooleanArray,
    ) : EncodingWorkflow

    class NonContiguous(
        val descriptorToWriterFieldIndex: IntArray,
    ) : EncodingWorkflow

    class MissingWriterFields(
        val missingWriterFields: List<Int>,
    ) : EncodingWorkflow
}

internal sealed interface DecodingStep {
    /**
     * This is a flag indicating that the element is deserializable.
     */
    sealed interface ValidatedDecodingStep : DecodingStep {
        val elementIndex: Int
        val schema: Schema
    }

    /**
     * The element is present in the writer schema and the class descriptor.
     */
    data class DeserializeWriterField(
        override val elementIndex: Int,
        val writerFieldIndex: Int,
        override val schema: Schema,
    ) : DecodingStep, ValidatedDecodingStep

    /**
     * The element is present in the class descriptor but not in the writer schema, so the default value is used.
     * Also:
     * - if the [com.github.avrokotlin.avro4k.AvroConfiguration.implicitNulls] is enabled, the default value is `null`.
     * - if the [com.github.avrokotlin.avro4k.AvroConfiguration.implicitEmptyCollections] is enabled, the default value is an empty array or map.
     */
    data class GetDefaultValue(
        override val elementIndex: Int,
        override val schema: Schema,
        val defaultValue: Any?,
    ) : DecodingStep, ValidatedDecodingStep

    /**
     * The element is present in the writer schema but not in the class descriptor, so it is skipped.
     */
    data class SkipWriterField(
        val writerFieldIndex: Int,
        val schema: Schema,
    ) : DecodingStep

    /**
     * The element is present in the class descriptor but not in the writer schema.
     * Also, the element don't have a default value but is optional, so we can skip the element.
     */
    data class IgnoreOptionalElement(
        val elementIndex: Int,
    ) : DecodingStep

    /**
     * The element is present in the class descriptor but not in the writer schema.
     * It does not have any default value and is not optional, so it fails as it cannot get any value for it.
     */
    data class MissingElementValueFailure(
        val elementIndex: Int,
    ) : DecodingStep
}

private fun AvroDefault.parseValueToGenericData(schema: Schema): Any? {
    if (value.isStartingAsJson()) {
        return Json.parseToJsonElement(value).convertDefaultToObject(schema)
    }
    return JsonPrimitive(value).convertDefaultToObject(schema)
}

private fun JsonElement.convertDefaultToObject(schema: Schema): Any? =
    when (this) {
        is JsonArray ->
            when (schema.type) {
                Schema.Type.ARRAY -> this.map { it.convertDefaultToObject(schema.elementType) }
                Schema.Type.UNION -> this.convertDefaultToObject(schema.resolveUnion(this, Schema.Type.ARRAY))
                else -> throw SerializationException("Not a valid array value for schema $schema: $this")
            }

        is JsonNull -> null
        is JsonObject ->
            when (schema.type) {
                Schema.Type.RECORD -> {
                    GenericData.Record(schema).apply {
                        entries.forEach { (fieldName, value) ->
                            val schemaField = schema.getField(fieldName)
                            put(schemaField.pos(), value.convertDefaultToObject(schemaField.schema()))
                        }
                    }
                }

                Schema.Type.MAP -> entries.associate { (key, value) -> key to value.convertDefaultToObject(schema.valueType) }
                Schema.Type.UNION -> this.convertDefaultToObject(schema.resolveUnion(this, Schema.Type.RECORD, Schema.Type.MAP))
                else -> throw SerializationException("Not a valid record value for schema $schema: $this")
            }

        is JsonPrimitive ->
            when (schema.type) {
                Schema.Type.BYTES -> this.content.toByteArray()
                Schema.Type.FIXED -> GenericData.Fixed(schema, this.content.toByteArray())
                Schema.Type.STRING -> this.content
                Schema.Type.ENUM -> this.content
                Schema.Type.BOOLEAN -> this.boolean

                Schema.Type.INT ->
                    when (schema.logicalType?.name) {
                        CHAR_LOGICAL_TYPE_NAME -> this.content.single().code
                        else -> this.int
                    }

                Schema.Type.LONG -> this.long
                Schema.Type.FLOAT -> this.float
                Schema.Type.DOUBLE -> this.double

                Schema.Type.UNION ->
                    this.convertDefaultToObject(
                        schema.resolveUnion(
                            this,
                            Schema.Type.BYTES,
                            Schema.Type.FIXED,
                            Schema.Type.STRING,
                            Schema.Type.ENUM,
                            Schema.Type.BOOLEAN,
                            Schema.Type.INT,
                            Schema.Type.LONG,
                            Schema.Type.FLOAT,
                            Schema.Type.DOUBLE
                        )
                    )

                else -> throw SerializationException("Not a valid primitive value for schema $schema: $this")
            }
    }

private fun Schema.resolveUnion(
    value: JsonElement?,
    vararg expectedTypes: Schema.Type,
): Schema {
    val index = types.indexOfFirst { it.type in expectedTypes }
    if (index < 0) {
        throw SerializationException("Union type does not contain one of ${expectedTypes.asList()}, unable to convert default value '$value' for schema $this")
    }
    return types[index]
}

private fun Schema.findFieldNamedOrAliasedAs(name: String): Schema.Field? =
    getField(name) ?: fields.firstOrNull { name in it.aliases() }