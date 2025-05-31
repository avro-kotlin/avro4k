package com.github.avrokotlin.avro4k

import com.github.avrokotlin.avro4k.internal.decodeWithApacheDecoder
import com.github.avrokotlin.avro4k.internal.encodeWithApacheEncoder
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.serializer
import okio.BufferedSink
import okio.BufferedSource
import org.apache.avro.Schema
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory

@Deprecated("Use kotlinx.io's encodeToSink instead")
@ExperimentalSerializationApi
public fun <T> Avro.encodeToSink(
    writerSchema: Schema,
    serializer: SerializationStrategy<T>,
    value: T,
    sink: BufferedSink,
) {
    val binaryEncoder = EncoderFactory.get().directBinaryEncoder(sink.outputStream(), null)
    encodeWithApacheEncoder(writerSchema, serializer, value, binaryEncoder)
    binaryEncoder.flush()
}

@Deprecated("Use kotlinx.io's encodeToSink instead")
@ExperimentalSerializationApi
public inline fun <reified T> Avro.encodeToSink(
    value: T,
    sink: BufferedSink,
) {
    val serializer = serializersModule.serializer<T>()
    encodeToSink(schema(serializer), serializer, value, sink)
}

@Deprecated("Use kotlinx.io's encodeToSink instead")
@ExperimentalSerializationApi
public inline fun <reified T> Avro.encodeToSink(
    writerSchema: Schema,
    value: T,
    sink: BufferedSink,
) {
    val serializer = serializersModule.serializer<T>()
    encodeToSink(writerSchema, serializer, value, sink)
}

@Deprecated("Use kotlinx.io's decodeFromSource instead")
@ExperimentalSerializationApi
public fun <T> Avro.decodeFromSource(
    writerSchema: Schema,
    deserializer: DeserializationStrategy<T>,
    source: BufferedSource,
): T {
    return decodeWithApacheDecoder(
        writerSchema,
        deserializer,
        DecoderFactory.get().directBinaryDecoder(source.inputStream(), null)
    )
}

@Deprecated("Use kotlinx.io's decodeFromSource instead")
@ExperimentalSerializationApi
public inline fun <reified T> Avro.decodeFromSource(source: BufferedSource): T {
    val serializer = serializersModule.serializer<T>()
    return decodeFromSource(schema(serializer.descriptor), serializer, source)
}

@Deprecated("Use kotlinx.io's decodeFromSource instead")
@ExperimentalSerializationApi
public inline fun <reified T> Avro.decodeFromSource(
    writerSchema: Schema,
    source: BufferedSource,
): T {
    val serializer = serializersModule.serializer<T>()
    return decodeFromSource(writerSchema, serializer, source)
}