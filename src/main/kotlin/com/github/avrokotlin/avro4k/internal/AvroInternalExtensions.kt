package com.github.avrokotlin.avro4k.internal

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.internal.decoder.direct.AvroValueDirectDecoder
import com.github.avrokotlin.avro4k.internal.encoder.direct.AvroValueDirectEncoder
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.SerializationStrategy
import org.apache.avro.Schema
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory

internal fun <T> Avro.encodeWithApacheEncoder(
    writerSchema: Schema,
    serializer: SerializationStrategy<T>,
    value: T,
    binaryEncoder: org.apache.avro.io.Encoder,
) {
    val apacheEncoder = if (configuration.validateSerialization) {
        EncoderFactory.get().validatingEncoder(writerSchema, binaryEncoder)
    } else {
        binaryEncoder
    }
    AvroValueDirectEncoder(writerSchema, this, apacheEncoder)
        .encodeSerializableValue(serializer, value)
}

internal fun <T> Avro.decodeWithApacheDecoder(
    writerSchema: Schema,
    deserializer: DeserializationStrategy<T>,
    binaryDecoder: org.apache.avro.io.Decoder,
): T {
    val apacheDecoder = if (configuration.validateSerialization) {
        DecoderFactory.get().validatingDecoder(writerSchema, binaryDecoder)
    } else {
        binaryDecoder
    }
    return AvroValueDirectDecoder(writerSchema, this, apacheDecoder)
        .decodeSerializableValue(deserializer)
}