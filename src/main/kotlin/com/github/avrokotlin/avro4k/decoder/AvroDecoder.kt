package com.github.avrokotlin.avro4k.decoder

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.encoding.Decoder
import org.apache.avro.Schema
import org.apache.avro.generic.GenericFixed

public interface AvroDecoder : Decoder {
    @ExperimentalSerializationApi
    public val currentWriterSchema: Schema

    @ExperimentalSerializationApi
    public fun decodeBytes(): ByteArray

    @ExperimentalSerializationApi
    public fun decodeFixed(): GenericFixed

    public fun decodeValue(): Any
}