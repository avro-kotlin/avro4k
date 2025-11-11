package com.github.avrokotlin.avro4k.internal.schema

import com.github.avrokotlin.avro4k.internal.AvroSchemaGenerationException
import kotlinx.serialization.descriptors.SerialDescriptor
import org.apache.avro.Schema

internal class MapVisitor(
    private val context: VisitorContext,
    private val onSchemaBuilt: (Schema) -> Unit,
) : SerialDescriptorMapVisitor {
    private lateinit var valueSchema: Schema

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
        if (it.isNullable()) {
            throw AvroSchemaGenerationException("Map key cannot be nullable. Actual generated map key schema: $it")
        }
        if (!it.isNonNullScalarType()) {
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
        onSchemaBuilt(Schema.createMap(valueSchema))
    }
}

internal fun Schema.isNonNullScalarType(): Boolean =
    when (type) {
        Schema.Type.BOOLEAN,
        Schema.Type.INT,
        Schema.Type.LONG,
        Schema.Type.FLOAT,
        Schema.Type.DOUBLE,
        Schema.Type.STRING,
        Schema.Type.ENUM,
        Schema.Type.BYTES,
        Schema.Type.FIXED,
        -> true

        Schema.Type.NULL,
        Schema.Type.ARRAY,
        Schema.Type.MAP,
        Schema.Type.RECORD,
        null,
        -> false

        Schema.Type.UNION -> types.all { it.isNonNullScalarType() }
    }