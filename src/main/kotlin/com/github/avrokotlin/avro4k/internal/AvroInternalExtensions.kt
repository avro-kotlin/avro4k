package com.github.avrokotlin.avro4k.internal

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.internal.decoder.direct.AvroValueDirectDecoder
import com.github.avrokotlin.avro4k.internal.encoder.direct.AvroValueDirectEncoder
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.SerializationStrategy
import org.apache.avro.Schema

internal fun <T> Avro.encodeWithBinaryEncoder(
    writerSchema: Schema,
    serializer: SerializationStrategy<T>,
    value: T,
    binaryEncoder: org.apache.avro.io.Encoder,
) {
    AvroValueDirectEncoder(writerSchema, this, binaryEncoder)
        .encodeSerializableValue(serializer, value)
}

internal fun <T> Avro.decodeWithBinaryDecoder(
    writerSchema: Schema,
    deserializer: DeserializationStrategy<T>,
    binaryDecoder: org.apache.avro.io.Decoder,
): T {
    return AvroValueDirectDecoder(writerSchema, this, binaryDecoder)
        .decodeSerializableValue(deserializer)
}