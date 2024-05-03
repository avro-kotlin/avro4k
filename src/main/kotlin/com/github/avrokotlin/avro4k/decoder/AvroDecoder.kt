package com.github.avrokotlin.avro4k.decoder

import kotlinx.serialization.encoding.Decoder
import org.apache.avro.Schema
import org.apache.avro.generic.GenericFixed

interface AvroDecoder : Decoder {
    val currentWriterSchema: Schema

    fun decodeBytes(): ByteArray

    fun decodeFixed(): GenericFixed

    fun decodeValue(): Any
}