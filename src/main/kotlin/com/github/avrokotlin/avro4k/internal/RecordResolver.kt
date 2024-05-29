package com.github.avrokotlin.avro4k.internal

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroAlias
import com.github.avrokotlin.avro4k.AvroDefault
import com.github.avrokotlin.avro4k.internal.schema.CHAR_LOGICAL_TYPE_NAME
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.elementNames
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
import org.apache.avro.util.WeakIdentityHashMap
import java.util.WeakHashMap

internal class RecordResolver(
    private val avro: Avro,
) {
    /**
     * For a class descriptor + writerSchema, it returns a map of the field index to the schema field.
     *
     * Note: We use the descriptor in the key as we could have multiple descriptors for the same record schema, and multiple record schemas for the same descriptor.
     */
    private val fieldCache: MutableMap<SerialDescriptor, MutableMap<Schema, ClassDescriptorForWriterSchema>> = WeakIdentityHashMap()

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
    ): ClassDescriptorForWriterSchema {
        if (classDescriptor.elementsCount == 0) {
            return ClassDescriptorForWriterSchema.EMPTY
        }
        return fieldCache.getOrPut(classDescriptor) { WeakHashMap() }.getOrPut(writerSchema) {
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
    ): ClassDescriptorForWriterSchema {
        val readerSchema = avro.schema(classDescriptor)

        val encodingSteps = computeEncodingSteps(classDescriptor, writerSchema)
        return ClassDescriptorForWriterSchema(
            sequentialEncoding = encodingSteps.areWriterFieldsSequentiallyOrdered(),
            computeDecodingSteps(classDescriptor, writerSchema, readerSchema),
            encodingSteps
        )
    }

    private fun Array<EncodingStep>.areWriterFieldsSequentiallyOrdered(): Boolean {
        var lastWriterFieldIndex = -1
        forEach { step ->
            when (step) {
                is EncodingStep.SerializeWriterField -> {
                    if (step.writerFieldIndex > lastWriterFieldIndex) {
                        lastWriterFieldIndex = step.writerFieldIndex
                    } else {
                        return false
                    }
                }

                is EncodingStep.MissingWriterFieldFailure -> {
                    if (step.writerFieldIndex > lastWriterFieldIndex) {
                        lastWriterFieldIndex = step.writerFieldIndex
                    } else {
                        return false
                    }
                }

                is EncodingStep.IgnoreElement -> {
                    // nothing to check
                }
            }
        }
        return true
    }

    private fun computeDecodingSteps(
        classDescriptor: SerialDescriptor,
        writerSchema: Schema,
        readerSchema: Schema,
    ): Array<DecodingStep> {
        val decodingSteps = mutableListOf<DecodingStep>()
        val elementIndexByWriterFieldIndex =
            classDescriptor.elementNames
                .mapIndexedNotNull { elementIndex, _ ->
                    writerSchema.tryGetField(avro.configuration.fieldNamingStrategy.resolve(classDescriptor, elementIndex), classDescriptor, elementIndex)
                        ?.let { it.pos() to elementIndex }
                }.toMap().toMutableMap()
        val visitedElements = BooleanArray(classDescriptor.elementsCount) { false }

        writerSchema.fields.forEachIndexed { writerFieldIndex, field ->
            decodingSteps += elementIndexByWriterFieldIndex.remove(writerFieldIndex)
                ?.let { elementIndex ->
                    visitedElements[elementIndex] = true
                    DecodingStep.DeserializeWriterField(
                        elementIndex = elementIndex,
                        writerFieldIndex = writerFieldIndex,
                        schema = field.schema()
                    )
                } ?: DecodingStep.SkipWriterField(writerFieldIndex, field.schema())
        }

        // iterate over remaining elements in the class descriptor that are not in the writer schema
        visitedElements.forEachIndexed { elementIndex, visited ->
            if (visited) return@forEachIndexed

            val readerDefaultAnnotation = classDescriptor.findElementAnnotation<AvroDefault>(elementIndex)
            // TODO try to fallback on the default value of the writer schema field if no readerDefaultAnnotation

            decodingSteps +=
                if (readerDefaultAnnotation != null) {
                    val elementSchema = readerSchema.fields[elementIndex].schema()
                    DecodingStep.GetDefaultValue(
                        elementIndex = elementIndex,
                        schema = elementSchema,
                        defaultValue = readerDefaultAnnotation.parseValueToGenericData(elementSchema)
                    )
                } else if (classDescriptor.isElementOptional(elementIndex)) {
                    DecodingStep.IgnoreOptionalElement(elementIndex)
                } else if (avro.configuration.implicitNulls &&
                    (
                        classDescriptor.getElementDescriptor(elementIndex).isNullable ||
                            classDescriptor.getElementDescriptor(elementIndex).isInline && classDescriptor.getElementDescriptor(elementIndex).getElementDescriptor(0).isNullable
                    )
                ) {
                    DecodingStep.GetDefaultValue(
                        elementIndex = elementIndex,
                        schema = NULL_SCHEMA,
                        defaultValue = null
                    )
                } else {
                    DecodingStep.MissingElementValueFailure(elementIndex)
                }
        }
        return decodingSteps.toTypedArray()
    }

    private fun computeEncodingSteps(
        classDescriptor: SerialDescriptor,
        writerSchema: Schema,
    ): Array<EncodingStep> {
        // Encoding steps are ordered regarding the class descriptor and not the writer schema.
        // Because kotlinx-serialization doesn't provide a way to encode non-sequentially elements.
        val encodingSteps = mutableListOf<EncodingStep>()
        val visitedWriterFields = BooleanArray(writerSchema.fields.size) { false }

        classDescriptor.elementNames.forEachIndexed { elementIndex, _ ->
            val avroFieldName = avro.configuration.fieldNamingStrategy.resolve(classDescriptor, elementIndex)
            val writerField = writerSchema.tryGetField(avroFieldName, classDescriptor, elementIndex)

            if (writerField != null) {
                visitedWriterFields[writerField.pos()] = true
                encodingSteps +=
                    EncodingStep.SerializeWriterField(
                        elementIndex = elementIndex,
                        writerFieldIndex = writerField.pos(),
                        schema = writerField.schema()
                    )
            } else {
                encodingSteps += EncodingStep.IgnoreElement(elementIndex)
            }
        }

        visitedWriterFields.forEachIndexed { writerFieldIndex, visited ->
            if (!visited) {
                encodingSteps += EncodingStep.MissingWriterFieldFailure(writerFieldIndex)
            }
        }

        return encodingSteps.toTypedArray()
    }

    private fun Schema.tryGetField(
        avroFieldName: String,
        classDescriptor: SerialDescriptor,
        elementIndex: Int,
    ): Schema.Field? =
        getField(avroFieldName)
            ?: fields.firstOrNull { avroFieldName in it.aliases() }
            ?: classDescriptor.findElementAnnotation<AvroAlias>(elementIndex)?.value?.let { aliases ->
                fields.firstOrNull { schemaField ->
                    schemaField.name() in aliases || schemaField.aliases().any { it in aliases }
                }
            }
}

internal class ClassDescriptorForWriterSchema(
    /**
     * If true, indicates that the encoding steps are ordered the same as the writer schema fields.
     * If false, indicates that the encoding steps are **NOT** ordered the same as the writer schema fields.
     */
    val sequentialEncoding: Boolean,
    /**
     * Decoding steps are ordered regarding the writer schema and not the class descriptor.
     */
    val decodingSteps: Array<DecodingStep>,
    /**
     * Encoding steps are ordered regarding the class descriptor and not the writer schema.
     */
    val encodingSteps: Array<EncodingStep>,
) {
    companion object {
        val EMPTY =
            ClassDescriptorForWriterSchema(
                sequentialEncoding = true,
                decodingSteps = emptyArray(),
                encodingSteps = emptyArray()
            )
    }
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
     * Also, if the [com.github.avrokotlin.avro4k.AvroConfiguration.implicitNulls] is enabled, the default value is `null`.
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

internal sealed interface EncodingStep {
    /**
     * The element is present in the writer schema and the class descriptor.
     */
    data class SerializeWriterField(
        val elementIndex: Int,
        val writerFieldIndex: Int,
        val schema: Schema,
    ) : EncodingStep

    /**
     * The element is present in the class descriptor but not in the writer schema, so the element is ignored as nothing has to be serialized.
     */
    data class IgnoreElement(
        val elementIndex: Int,
    ) : EncodingStep

    /**
     * The writer field doesn't have a corresponding element in the class descriptor, so we aren't able to serialize a value.
     */
    data class MissingWriterFieldFailure(
        val writerFieldIndex: Int,
    ) : EncodingStep
}

private fun AvroDefault.parseValueToGenericData(schema: Schema): Any? {
    if (value.isStartingAsJson()) {
        return Json.parseToJsonElement(value).convertDefaultToObject(schema)
    }
    return JsonPrimitive(value).convertDefaultToObject(schema)
}

private fun JsonElement.convertDefaultToObject(schema: Schema): Any? {
    return when (this) {
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

private val NULL_SCHEMA = Schema.create(Schema.Type.NULL)