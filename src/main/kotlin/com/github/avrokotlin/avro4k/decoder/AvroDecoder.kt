package com.github.avrokotlin.avro4k.decoder

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.encoding.Decoder
import org.apache.avro.Schema
import org.apache.avro.generic.GenericFixed

public interface AvroDecoder : Decoder {
    /**
     * Provides the schema used to encode the current value.
     * It won't return a union as the schema correspond to the actual value.
     */
    @ExperimentalSerializationApi
    public val currentWriterSchema: Schema

    @ExperimentalSerializationApi
    public fun decodeBytes(): ByteArray

    @ExperimentalSerializationApi
    public fun decodeFixed(): GenericFixed

    /**
     * Decode a value that corresponds to the [currentWriterSchema].
     *
     * You should prefer using directly [currentWriterSchema] to get the schema and then decode the value using the appropriate **decode*** method.
     */
    @ExperimentalSerializationApi
    public fun decodeValue(): Any
}