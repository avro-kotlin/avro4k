package com.github.avrokotlin.avro4k

import com.github.avrokotlin.avro4k.internal.decodeWithApacheDecoder
import com.github.avrokotlin.avro4k.internal.encodeWithApacheEncoder
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
    val avroEncoder = EncoderFactory.get().binaryEncoder(outputStream, null)
    encodeWithApacheEncoder(writerSchema, serializer, value, avroEncoder)
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
    return decodeWithApacheDecoder(writerSchema, deserializer, DecoderFactory.get().binaryDecoder(inputStream, null))
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