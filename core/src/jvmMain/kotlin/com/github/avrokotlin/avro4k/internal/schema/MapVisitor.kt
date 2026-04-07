package com.github.avrokotlin.avro4k.internal.schema

import com.github.avrokotlin.avro4k.AvroSchema
import com.github.avrokotlin.avro4k.AvroSchema.MapSchema
import com.github.avrokotlin.avro4k.AvroSchema.UnionSchema
import com.github.avrokotlin.avro4k.internal.AvroSchemaGenerationException
import kotlinx.serialization.descriptors.SerialDescriptor

internal class MapVisitor(
    private val context: VisitorContext,
    private val onSchemaBuilt: (AvroSchema) -> Unit,
) : SerialDescriptorMapVisitor {
    private lateinit var valueSchema: AvroSchema

    override fun visitMapKey(
        mapDescriptor: SerialDescriptor,
        keyElementIndex: Int,
    ) = ValueVisitor(context) {
        // In avro, the map key must be a string.
        // Here we just delegate the schema building to the value visitor
        // and then check if the output schema is about a type that we can
        // stringify (e.g. when .toString() makes sense).
        // Here we are just checking if the schema is string-compatible. We don't need to
        // store the schema as it is always a string.
        if (!it.isAcceptedMapKeyType()) {
            throw AvroSchemaGenerationException("Map key must be a non-null scalar type (e.g. not a record, map or array). Actual generated map key schema: $it")
        }
    }

    override fun visitMapValue(
        mapDescriptor: SerialDescriptor,
        valueElementIndex: Int,
    ) = ValueVisitor(context) {
        valueSchema = it
    }

    override fun endMapVisit(descriptor: SerialDescriptor) {
        onSchemaBuilt(MapSchema(valueSchema))
    }
}

internal fun AvroSchema.isAcceptedMapKeyType(): Boolean =
    isNonNullScalarType()
private fun AvroSchema.isNonNullScalarType(): Boolean =
    when (type) {
        AvroSchema.Type.BOOLEAN,
        AvroSchema.Type.INT,
        AvroSchema.Type.LONG,
        AvroSchema.Type.FLOAT,
        AvroSchema.Type.DOUBLE,
        AvroSchema.Type.STRING,
        AvroSchema.Type.ENUM,
        AvroSchema.Type.BYTES,
        AvroSchema.Type.FIXED,
            -> true

        AvroSchema.Type.NULL,
        AvroSchema.Type.ARRAY,
        AvroSchema.Type.MAP,
        AvroSchema.Type.RECORD,
            -> false

        AvroSchema.Type.UNION -> (this as UnionSchema).types.all { it.isNonNullScalarType() }
    }