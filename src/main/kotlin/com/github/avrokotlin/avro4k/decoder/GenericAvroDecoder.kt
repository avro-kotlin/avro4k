package com.github.avrokotlin.avro4k.decoder

import com.github.avrokotlin.avro4k.AvroConfiguration
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema

@ExperimentalSerializationApi
class GenericAvroDecoder(
    private val value: Any?,
    override val currentSchema: Schema,
    override val serializersModule: SerializersModule,
    override val configuration: AvroConfiguration,
) : AbstractAvroDecoder() {
    @ExperimentalSerializationApi
    override fun decodeNotNullMark() = value != null

    override fun decodeAny() = value!!
}
