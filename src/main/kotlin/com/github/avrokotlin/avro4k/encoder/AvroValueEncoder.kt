package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.internal.BadEncodedValueError
import org.apache.avro.Schema

internal class AvroValueEncoder(
    override val avro: Avro,
    override var currentWriterSchema: Schema,
    private val onEncoded: (Any?) -> Unit,
) : AbstractAvroEncoder() {
    override fun encodeValue(value: Any) {
        onEncoded(value)
    }

    override fun encodeNull() {
        encodeResolvingUnion(
            { BadEncodedValueError(null, currentWriterSchema, Schema.Type.NULL) }
        ) {
            when (it.type) {
                Schema.Type.NULL -> {
                    { onEncoded(null) }
                }
                else -> null
            }
        }
    }
}