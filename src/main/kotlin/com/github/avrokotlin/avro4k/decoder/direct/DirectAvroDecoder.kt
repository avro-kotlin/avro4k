package com.github.avrokotlin.avro4k.decoder.direct

import com.github.avrokotlin.avro4k.schema.Resolver
import kotlinx.serialization.encoding.Decoder

interface DirectAvroDecoder : Decoder {
    val currentAction: Resolver.Action
    fun decodeFixed() : ByteArray
    fun decodeBytes() : ByteArray
}