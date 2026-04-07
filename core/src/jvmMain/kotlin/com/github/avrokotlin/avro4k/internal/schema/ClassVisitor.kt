package com.github.avrokotlin.avro4k.internal.schema

import com.github.avrokotlin.avro4k.AvroSchema
import com.github.avrokotlin.avro4k.AvroSchema.RecordSchema
import com.github.avrokotlin.avro4k.AvroSchema.Type
import com.github.avrokotlin.avro4k.AvroSchema.UnionSchema
import com.github.avrokotlin.avro4k.LockableList
import com.github.avrokotlin.avro4k.Name
import com.github.avrokotlin.avro4k.ResolvedSchema
import com.github.avrokotlin.avro4k.internal.jsonElement
import com.github.avrokotlin.avro4k.internal.nonNullSerialName
import com.github.avrokotlin.avro4k.isNullable
import com.github.avrokotlin.avro4k.serializer.ElementLocation
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

internal class ClassVisitor(
    descriptor: SerialDescriptor,
    private val context: VisitorContext,
    private val onSchemaBuilt: (AvroSchema) -> Unit,
) : SerialDescriptorClassVisitor {
    private val fields = LockableList<RecordSchema.Field>()
    private val schemaAlreadyResolved: Boolean
    private val schema: AvroSchema

    init {
        var schemaAlreadyResolved = true
        schema =
            context.resolvedSchemas.getOrPut(descriptor.nonNullSerialName) {
                schemaAlreadyResolved = false

                val annotations = TypeAnnotations(descriptor)
                val schema =
                    RecordSchema(
                        name = Name(descriptor.nonNullSerialName),
                        doc = annotations.doc?.value,
                        aliases = annotations.aliases?.value?.map { Name(it) }?.toSet() ?: emptySet(),
                        props = annotations.props.associate { it.key to it.jsonElement },
                        fields = fields
                    )
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
        fields.lock()
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
        elementSchema: AvroSchema,
    ): RecordSchema.Field {
        val (finalSchema, fieldDefault) = getDefaultAndReorderUnionIfNeeded(annotations, elementSchema)

        return RecordSchema.Field(
            name = fieldName,
            schema = finalSchema,
            doc = annotations.doc?.value,
            defaultValue = fieldDefault,
            aliases = annotations.aliases?.value?.toSet() ?: emptySet(),
            props = annotations.props.associate { it.key to it.jsonElement }
        )
    }

    private fun getDefaultAndReorderUnionIfNeeded(
        annotations: FieldAnnotations,
        elementSchema: AvroSchema,
    ): Pair<AvroSchema, JsonElement?> {
        val defaultValue = annotations.default?.jsonElement
        if (defaultValue == null) {
            // No default value, let's make implicit default
            if (context.configuration.implicitNulls && elementSchema.isNullable) {
                return elementSchema.moveToHeadOfUnion { it.type == Type.NULL } to JsonNull
            } else if (context.configuration.implicitEmptyCollections) {
                (if (elementSchema is UnionSchema) elementSchema.types else listOf(elementSchema)).forEachIndexed { index, schema ->
                    if (schema.type == Type.ARRAY) {
                        return elementSchema.moveToHeadOfUnion(index) to JsonArray(emptyList())
                    }
                    if (schema.type == Type.MAP) {
                        return elementSchema.moveToHeadOfUnion(index) to JsonObject(emptyMap())
                    }
                }
            }
        } else if (defaultValue === JsonNull) {
            // If the user sets "null" but the field is not nullable, maybe the user wanted to set the "null" string default
            val finalSchema = elementSchema.moveToHeadOfUnion { it.type == Type.NULL }
            val adaptedDefault = if (!elementSchema.isNullable) JsonPrimitive("null") else defaultValue
            return finalSchema to adaptedDefault
        } else if (elementSchema.isNullable) {
            // default is not null, so let's just put the null schema at the end of the union which should cover the main use cases
            return elementSchema.moveToTailOfUnion { it.type === Type.NULL } to defaultValue
        }
        return elementSchema to defaultValue
    }
}

private fun AvroSchema.moveToHeadOfUnion(predicate: (ResolvedSchema) -> Boolean): AvroSchema {
    if (this !is UnionSchema) {
        return this
    }
    types.indexOfFirst(predicate).let { index ->
        if (index == -1) {
            return this
        }
        return moveToHeadOfUnion(index)
    }
}

private fun AvroSchema.moveToHeadOfUnion(typeIndex: Int): AvroSchema {
    if (this !is UnionSchema || typeIndex >= types.size) {
        return this
    }
    return UnionSchema(types.toMutableList().apply { add(0, removeAt(typeIndex)) })
}

private fun AvroSchema.moveToTailOfUnion(predicate: (AvroSchema) -> Boolean): AvroSchema {
    if (this !is UnionSchema) {
        return this
    }
    types.indexOfFirst(predicate).let { index ->
        if (index == -1) {
            return this
        }
        return moveToTailOfUnion(index)
    }
}

private fun AvroSchema.moveToTailOfUnion(typeIndex: Int): AvroSchema {
    if (this !is UnionSchema || typeIndex >= types.size) {
        return this
    }
    return UnionSchema(types.toMutableList().apply { add(removeAt(typeIndex)) })
}