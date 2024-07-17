package com.github.avrokotlin.avro4k

import com.github.avrokotlin.avro4k.internal.decodeWithBinaryDecoder
import com.github.avrokotlin.avro4k.internal.encodeWithBinaryEncoder
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.serializer
import okio.BufferedSink
import okio.BufferedSource
import org.apache.avro.Schema
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory

@ExperimentalSerializationApi
public fun <T> Avro.encodeToSink(
    writerSchema: Schema,
    serializer: SerializationStrategy<T>,
    value: T,
    sink: BufferedSink,
) {
    val avroEncoder =
        EncoderFactory.get().directBinaryEncoder(sink.outputStream(), null).let {
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
public inline fun <reified T> Avro.encodeToSink(
    value: T,
    sink: BufferedSink,
) {
    val serializer = serializersModule.serializer<T>()
    encodeToSink(schema(serializer), serializer, value, sink)
}

@ExperimentalSerializationApi
public inline fun <reified T> Avro.encodeToSink(
    writerSchema: Schema,
    value: T,
    sink: BufferedSink,
) {
    val serializer = serializersModule.serializer<T>()
    encodeToSink(writerSchema, serializer, value, sink)
}

@ExperimentalSerializationApi
public fun <T> Avro.decodeFromSource(
    writerSchema: Schema,
    deserializer: DeserializationStrategy<T>,
    source: BufferedSource,
): T {
    val avroDecoder =
        DecoderFactory.get().directBinaryDecoder(source.inputStream(), null).let {
            if (configuration.validateSerialization) {
                DecoderFactory.get().validatingDecoder(writerSchema, it)
            } else {
                it
            }
        }

    return decodeWithBinaryDecoder(writerSchema, deserializer, avroDecoder)
}

@ExperimentalSerializationApi
public inline fun <reified T> Avro.decodeFromSource(source: BufferedSource): T {
    val serializer = serializersModule.serializer<T>()
    return decodeFromSource(schema(serializer.descriptor), serializer, source)
}

@ExperimentalSerializationApi
public inline fun <reified T> Avro.decodeFromSource(
    writerSchema: Schema,
    source: BufferedSource,
): T {
    val serializer = serializersModule.serializer<T>()
    return decodeFromSource(writerSchema, serializer, source)
}