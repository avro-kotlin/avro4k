package com.github.avrokotlin.avro4k.decoder

import com.github.avrokotlin.avro4k.Avro
import kotlinx.serialization.ExperimentalSerializationApi
import org.apache.avro.Schema

@ExperimentalSerializationApi
class GenericAvroDecoder(
    private val value: Any?,
    override val currentSchema: Schema,
    override val avro: Avro,
) : AbstractAvroDecoder() {
    @ExperimentalSerializationApi
    override fun decodeNotNullMark() = value != null

    override fun decodeAny() = value!!
}
