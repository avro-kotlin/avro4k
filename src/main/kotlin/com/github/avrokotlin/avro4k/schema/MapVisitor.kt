package com.github.avrokotlin.avro4k.schema

import kotlinx.serialization.descriptors.SerialDescriptor
import org.apache.avro.Schema

internal class MapVisitor(
    override val context: VisitorContext,
    private val onSchemaBuilt: (Schema) -> Unit,
) : SerialDescriptorMapVisitor, AvroVisitorContextAware {
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
        // store the schema as it is a string.
        if (it.isNullable()) {
            throw AvroSchemaGenerationException("Map key cannot be nullable. Actual generated map key schema: $it")
        }
        if (!it.isStringable()) {
            throw AvroSchemaGenerationException("Map key must be string-able (boolean, number, enum, or string). Actual generated map key schema: $it")
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

private fun Schema.isStringable(): Boolean =
    when (type) {
        Schema.Type.BOOLEAN,
        Schema.Type.INT,
        Schema.Type.LONG,
        Schema.Type.FLOAT,
        Schema.Type.DOUBLE,
        Schema.Type.STRING,
        Schema.Type.ENUM,
        -> true

        Schema.Type.NULL,
        Schema.Type.BYTES, // bytes could be stringified, but it's not a good idea as it can produce unreadable strings.
        Schema.Type.FIXED, // same, just bytes. Btw, if the user wants to stringify it, he can use @Contextual or custom @Serializable serializer.
        Schema.Type.ARRAY,
        Schema.Type.MAP,
        Schema.Type.RECORD,
        null,
        -> false

        Schema.Type.UNION -> types.all { it.isStringable() }
    }