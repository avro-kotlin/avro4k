package com.github.avrokotlin.avro4k.decoder

import kotlinx.serialization.encoding.Decoder
import org.apache.avro.Schema

interface ExtendedDecoder : Decoder {
    fun decodeAny(): Any
    val currentSchema: Schema
}