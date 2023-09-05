package com.github.avrokotlin.avro4k.decoder

import kotlinx.serialization.encoding.Decoder
import org.apache.avro.Schema

interface NativeAvroDecoder : Decoder {
    fun decodeAny(): Any
    val currentSchema: Schema
}