package com.github.avrokotlin.avro4k.internal.encoder.generic

import com.github.avrokotlin.avro4k.Avro
import org.apache.avro.Schema

internal class AvroValueGenericEncoder(
    override val avro: Avro,
    override var currentWriterSchema: Schema,
    private val onEncoded: (Any?) -> Unit,
) : AbstractAvroGenericEncoder() {
    override fun encodeValue(value: Any) {
        onEncoded(value)
    }

    override fun encodeNullUnchecked() {
        onEncoded(null)
    }
}