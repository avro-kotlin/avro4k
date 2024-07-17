package com.github.avrokotlin.avro4k.internal.encoder.generic

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.encodeResolving
import com.github.avrokotlin.avro4k.internal.BadEncodedValueError
import org.apache.avro.Schema

internal class AvroValueGenericEncoder(
    override val avro: Avro,
    override var currentWriterSchema: Schema,
    private val onEncoded: (Any?) -> Unit,
) : AbstractAvroGenericEncoder() {
    override fun encodeValue(value: Any) {
        onEncoded(value)
    }

    override fun encodeNull() {
        encodeResolving(
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