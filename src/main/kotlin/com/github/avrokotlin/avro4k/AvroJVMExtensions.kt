package com.github.avrokotlin.avro4k

import com.github.avrokotlin.avro4k.internal.decodeWithBinaryDecoder
import com.github.avrokotlin.avro4k.internal.encodeWithBinaryEncoder
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.serializer
import org.apache.avro.Schema
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
import java.io.InputStream
import java.io.OutputStream

@ExperimentalSerializationApi
public fun <T> Avro.encodeToStream(
    writerSchema: Schema,
    serializer: SerializationStrategy<T>,
    value: T,
    outputStream: OutputStream,
) {
    val avroEncoder =
        EncoderFactory.get().directBinaryEncoder(outputStream, null).let {
            if (configuration.validateSerialization) {
                EncoderFactory.get().validatingEncoder(writerSchema, it)
            } else {
                it
            }
        }

    encodeWithBinaryEncoder(writerSchema, serializer, value, avroEncoder)

    avroEncoder.flush()
}

@ExperimentalSerializationApi
public inline fun <reified T> Avro.encodeToStream(
    value: T,
    outputStream: OutputStream,
) {
    val serializer = serializersModule.serializer<T>()
    encodeToStream(schema(serializer), serializer, value, outputStream)
}

@ExperimentalSerializationApi
public inline fun <reified T> Avro.encodeToStream(
    writerSchema: Schema,
    value: T,
    outputStream: OutputStream,
) {
    val serializer = serializersModule.serializer<T>()
    encodeToStream(writerSchema, serializer, value, outputStream)
}

@ExperimentalSerializationApi
public fun <T> Avro.decodeFromStream(
    writerSchema: Schema,
    deserializer: DeserializationStrategy<T>,
    inputStream: InputStream,
): T {
    val avroDecoder =
        DecoderFactory.get().directBinaryDecoder(inputStream, null).let {
            if (configuration.validateSerialization) {
                DecoderFactory.get().validatingDecoder(writerSchema, it)
            } else {
                it
            }
        }

    return decodeWithBinaryDecoder(writerSchema, deserializer, avroDecoder)
}

@ExperimentalSerializationApi
public inline fun <reified T> Avro.decodeFromStream(inputStream: InputStream): T {
    val serializer = serializersModule.serializer<T>()
    return decodeFromStream(schema(serializer.descriptor), serializer, inputStream)
}

@ExperimentalSerializationApi
public inline fun <reified T> Avro.decodeFromStream(
    writerSchema: Schema,
    inputStream: InputStream,
): T {
    val serializer = serializersModule.serializer<T>()
    return decodeFromStream(writerSchema, serializer, inputStream)
}