package com.github.avrokotlin.avro4k.internal.schema

import com.github.avrokotlin.avro4k.AvroDefault
import com.github.avrokotlin.avro4k.internal.asSchemaList
import com.github.avrokotlin.avro4k.internal.isStartingAsJson
import com.github.avrokotlin.avro4k.internal.jsonNode
import com.github.avrokotlin.avro4k.internal.nonNullSerialName
import com.github.avrokotlin.avro4k.serializer.ElementLocation
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.boolean
import kotlinx.serialization.json.booleanOrNull
import org.apache.avro.JsonProperties
import org.apache.avro.Schema

internal class ClassVisitor(
    descriptor: SerialDescriptor,
    private val context: VisitorContext,
    private val onSchemaBuilt: (Schema) -> Unit,
) : SerialDescriptorClassVisitor {
    private val fields = mutableListOf<Schema.Field>()
    private val schemaAlreadyResolved: Boolean
    private val schema: Schema

    init {
        var schemaAlreadyResolved = true
        schema =
            context.resolvedSchemas.getOrPut(descriptor.nonNullSerialName) {
                schemaAlreadyResolved = false

                val annotations = TypeAnnotations(descriptor)
                val schema =
                    Schema.createRecord(
                        // name =
                        descriptor.nonNullSerialName,
                        // doc =
                        annotations.doc?.value,
                        // namespace =
                        null,
                        // isError =
                        false
                    )
                annotations.aliases?.value?.forEach { schema.addAlias(it) }
                annotations.props.forEach { schema.addProp(it.key, it.jsonNode) }
                schema
            }
        this.schemaAlreadyResolved = schemaAlreadyResolved
    }

    override fun visitClassElement(
        descriptor: SerialDescriptor,
        elementIndex: Int,
    ): SerialDescriptorValueVisitor? {
        if (schemaAlreadyResolved) {
            return null
        }
        return ValueVisitor(context.copy(inlinedElements = listOf(ElementLocation(descriptor, elementIndex)))) { fieldSchema ->
            fields.add(
                createField(
                    context.avro.configuration.fieldNamingStrategy.resolve(descriptor, elementIndex),
                    FieldAnnotations(descriptor, elementIndex),
                    fieldSchema
                )
            )
        }
    }

    override fun endClassVisit(descriptor: SerialDescriptor) {
        if (!schemaAlreadyResolved) {
            schema.fields = fields
        }
        onSchemaBuilt(schema)
    }

    /**
     * Create a field with the given annotations.
     * Here are managed the generic field level annotations:
     * - namespaceOverride
     * - default (also sort unions according to the default value)
     * - aliases
     * - doc
     * - props & json props
     */
    private fun createField(
        fieldName: String,
        annotations: FieldAnnotations,
        elementSchema: Schema,
    ): Schema.Field {
        val (finalSchema, fieldDefault) = getDefaultAndReorderUnionIfNeeded(annotations, elementSchema)

        val field =
            Schema.Field(
                fieldName,
                finalSchema,
                annotations.doc?.value,
                fieldDefault
            )
        annotations.aliases?.value?.forEach { field.addAlias(it) }
        annotations.props.forEach { field.addProp(it.key, it.jsonNode) }
        return field
    }

    private fun getDefaultAndReorderUnionIfNeeded(
        annotations: FieldAnnotations,
        elementSchema: Schema,
    ): Pair<Schema, Any?> {
        val defaultValue = annotations.default?.toAvroObject()
        if (defaultValue == null) {
            if (context.configuration.implicitNulls && elementSchema.isNullable) {
                return elementSchema.moveToHeadOfUnion { it.type == Schema.Type.NULL } to JsonProperties.NULL_VALUE
            } else if (context.configuration.implicitEmptyCollections) {
                elementSchema.asSchemaList().forEachIndexed { index, schema ->
                    if (schema.type == Schema.Type.ARRAY) {
                        return elementSchema.moveToHeadOfUnion(index) to emptyList<Any>()
                    }
                    if (schema.type == Schema.Type.MAP) {
                        return elementSchema.moveToHeadOfUnion(index) to emptyMap<Any, Any>()
                    }
                }
            }
        } else if (defaultValue === JsonProperties.NULL_VALUE) {
            // If the user sets "null" but the field is not nullable, maybe the user wanted to set the "null" string default
            val finalSchema = elementSchema.moveToHeadOfUnion { it.type == Schema.Type.NULL }
            val adaptedDefault = if (!elementSchema.isNullable) "null" else defaultValue
            return finalSchema to adaptedDefault
        } else if (elementSchema.asSchemaList().any { it.logicalType?.name == CHAR_LOGICAL_TYPE_NAME }) {
            // requires a string default value with exactly 1 character, and map the character to the char code as it is an int
            if (defaultValue is String && defaultValue.length == 1) {
                return elementSchema.moveToHeadOfUnion { it.logicalType?.name == CHAR_LOGICAL_TYPE_NAME } to defaultValue.single().code
            } else {
                throw SerializationException("Default value for Char must be a single character string. Invalid value of type ${defaultValue::class.qualifiedName}: $defaultValue")
            }
        } else if (elementSchema.isNullable) {
            // default is not null, so let's just put the null schema at the end of the union which should cover the main use cases
            return elementSchema.moveToTailOfUnion { it.type === Schema.Type.NULL } to defaultValue
        }
        return elementSchema to defaultValue
    }
}

private fun AvroDefault.toAvroObject(): Any {
    if (value.isStartingAsJson()) {
        return Json.parseToJsonElement(value).toAvroObject()
    }
    return value
}

private fun JsonElement.toAvroObject(): Any =
    when (this) {
        is JsonNull -> JsonProperties.NULL_VALUE
        is JsonObject -> this.entries.associate { it.key to it.value.toAvroObject() }
        is JsonArray -> this.map { it.toAvroObject() }
        is JsonPrimitive ->
            when {
                this.isString -> this.content
                this.booleanOrNull != null -> this.boolean
                else -> {
                    this.content.toBigDecimal().stripTrailingZeros().let {
                        if (it.scale() <= 0) {
                            it.toBigInteger()
                        } else {
                            it
                        }
                    }
                }
            }
    }

private fun Schema.moveToHeadOfUnion(predicate: (Schema) -> Boolean): Schema {
    if (!isUnion) {
        return this
    }
    types.indexOfFirst(predicate).let { index ->
        if (index == -1) {
            return this
        }
        return moveToHeadOfUnion(index)
    }
}

private fun Schema.moveToHeadOfUnion(typeIndex: Int): Schema {
    if (!isUnion || typeIndex >= types.size) {
        return this
    }
    return Schema.createUnion(types.toMutableList().apply { add(0, removeAt(typeIndex)) })
}

private fun Schema.moveToTailOfUnion(predicate: (Schema) -> Boolean): Schema {
    if (!isUnion) {
        return this
    }
    types.indexOfFirst(predicate).let { index ->
        if (index == -1) {
            return this
        }
        return moveToTailOfUnion(index)
    }
}

private fun Schema.moveToTailOfUnion(typeIndex: Int): Schema {
    if (!isUnion || typeIndex >= types.size) {
        return this
    }
    return Schema.createUnion(types.toMutableList().apply { add(removeAt(typeIndex)) })
}