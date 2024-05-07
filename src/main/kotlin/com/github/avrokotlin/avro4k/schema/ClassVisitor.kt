package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.AvroDefault
import com.github.avrokotlin.avro4k.internal.isStartingAsJson
import com.github.avrokotlin.avro4k.internal.jsonNode
import com.github.avrokotlin.avro4k.internal.nonNullSerialName
import com.github.avrokotlin.avro4k.internal.overrideNamespace
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
        return ValueVisitor(
            context.copy(
                inlinedAnnotations = ValueAnnotations(descriptor, elementIndex)
            )
        ) {
            fields.add(
                createField(
                    context.avro.configuration.fieldNamingStrategy.resolve(descriptor, elementIndex),
                    FieldAnnotations(descriptor, elementIndex),
                    it
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
        var finalSchema: Schema = annotations.namespaceOverride?.value?.let { elementSchema.overrideNamespace(it) } ?: elementSchema

        val fieldDefault = getFieldDefault(annotations.default, finalSchema)

        if (fieldDefault != null) {
            reorderUnionIfNeeded(fieldDefault, finalSchema)?.let {
                finalSchema = it
            }
        }

        val field =
            Schema.Field(
                // name =
                fieldName,
                // schema =
                finalSchema,
                // doc =
                annotations.doc?.value,
                // defaultValue =
                fieldDefault
            )
        annotations.aliases.flatMap { it.value.asSequence() }.forEach { field.addAlias(it) }
        annotations.props.forEach { field.addProp(it.key, it.jsonNode) }
        return field
    }

    /**
     * Reorder the union to put the non-null first if the default value is non-null.
     */
    private fun reorderUnionIfNeeded(
        fieldDefault: Any,
        finalSchema: Schema,
    ): Schema? {
        if (finalSchema.isUnion && finalSchema.isNullable) {
            var nullNotFirst = false
            if (fieldDefault is Collection<*>) {
                nullNotFirst = fieldDefault.any { it != JsonProperties.NULL_VALUE }
            } else if (fieldDefault != JsonProperties.NULL_VALUE) {
                nullNotFirst = true
            }
            if (nullNotFirst) {
                val nullIndex = finalSchema.types.indexOfFirst { it.type == Schema.Type.NULL }
                val nonNullTypes = finalSchema.types.toMutableList()
                val nullType = nonNullTypes.removeAt(nullIndex)
                return Schema.createUnion(nonNullTypes + nullType)
            }
        }
        return null
    }

    private fun getFieldDefault(
        default: AvroDefault?,
        fieldSchema: Schema,
    ): Any? {
        val defaultValue = default?.jsonValue

        if (defaultValue == null && context.avro.configuration.implicitNulls && fieldSchema.isNullable) {
            return JsonProperties.NULL_VALUE
        }
        return defaultValue
    }
}

private val AvroDefault.jsonValue: Any
    get() {
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