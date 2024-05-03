package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.Avro
import kotlinx.serialization.descriptors.SerialDescriptor
import org.apache.avro.Schema

internal class AvroValueEncoder(
    override val avro: Avro,
    schema: Schema,
    private val onEncoded: (Any?) -> Unit,
) : AvroTaggedEncoder<Schema>() {
    override val Schema.writerSchema: Schema
        get() = this

    init {
        pushTag(schema)
    }

    override fun SerialDescriptor.getTag(index: Int): Schema {
        throw UnsupportedOperationException("${this::class} does not support element encoding")
    }

    override fun encodeTaggedValue(
        tag: Schema,
        value: Any,
    ) {
        onEncoded(value)
    }

    override fun encodeTaggedNull(tag: Schema) {
        require(tag.writerSchema.isNullable)
        onEncoded(null)
    }
}