package com.github.avrokotlin.avro4k

import com.github.avrokotlin.avro4k.internal.decodeWithApacheDecoder
import kotlinx.io.asSink
import kotlinx.io.asSource
import kotlinx.io.buffered
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.serializer
import org.apache.avro.Schema
import org.apache.avro.io.DecoderFactory
import org.apache.avro.util.ByteBufferInputStream
import java.io.InputStream
import java.io.OutputStream
import java.nio.ByteBuffer

@Deprecated("Use encodeToSink instead", ReplaceWith("encodeToSink(writerSchema, serializer, value, outputStream.asSink().buffered())"))
@ExperimentalAvro4kApi
public fun <T> Avro.encodeToStream(
    writerSchema: Schema,
    serializer: SerializationStrategy<T>,
    value: T,
    outputStream: OutputStream,
) {
    encodeToSink(writerSchema, serializer, value, outputStream.asSink().buffered())
}

@Deprecated("Use encodeToSink instead", replaceWith = ReplaceWith("encodeToSink(value, outputStream.asSink().buffered())"))
@ExperimentalAvro4kApi
public inline fun <reified T> Avro.encodeToStream(
    value: T,
    outputStream: OutputStream,
) {
    encodeToSink(value, outputStream.asSink().buffered())
}

@Deprecated("Use encodeToSink instead", replaceWith = ReplaceWith("encodeToSink(writerSchema, value, outputStream.asSink().buffered())"))
@ExperimentalAvro4kApi
public inline fun <reified T> Avro.encodeToStream(
    writerSchema: Schema,
    value: T,
    outputStream: OutputStream,
) {
    encodeToSink(writerSchema, value, outputStream.asSink().buffered())
}

@Deprecated("Use decodeFromSource instead", replaceWith = ReplaceWith("decodeFromSource(deserializer, inputStream.asSource().buffered())"))
@ExperimentalAvro4kApi
public fun <T> Avro.decodeFromStream(
    writerSchema: Schema,
    deserializer: DeserializationStrategy<T>,
    inputStream: InputStream,
): T {
    return decodeFromSource(writerSchema, deserializer, inputStream.asSource().buffered())
}

@Deprecated("Use decodeFromSource instead", replaceWith = ReplaceWith("decodeFromSource(inputStream.asSource().buffered())"))
@ExperimentalAvro4kApi
public inline fun <reified T> Avro.decodeFromStream(inputStream: InputStream): T {
    return decodeFromSource(inputStream.asSource().buffered())
}

@Deprecated("Use decodeFromSource instead", replaceWith = ReplaceWith("decodeFromSource(writerSchema, inputStream.asSource().buffered())"))
@ExperimentalAvro4kApi
public inline fun <reified T> Avro.decodeFromStream(
    writerSchema: Schema,
    inputStream: InputStream,
): T {
    return decodeFromSource(writerSchema, inputStream.asSource().buffered())
}

/**
 * Decode the value [T] from the given ByteBuffer, helpful when expecting zero-copy decoding from the jvm.
 * The [input] MUST follow the [writerSchema] to ensure the bytes to be readable, or it may end up to unexpected results or an exception.
 *
 * @param input the ByteBuffer to decode from. The [ByteBuffer.position] will be updated based on the decoded data bytes length.
 * @param deserializer the deserialization strategy to use for decoding the value. It is inferred from [T] based on [Avro.serializersModule] if not provided.
 * @param writerSchema the schema to use for decoding the value. It is highly recommended to set this schema if the [T] type is not stable across serializations to ensure the
 *                     bytes to be readable. It is inferred from [T] using [Avro.schema] if not provided.
 */
@ExperimentalAvro4kApi
public inline fun <reified T> Avro.decodeFromByteBuffer(
    input: ByteBuffer,
    deserializer: DeserializationStrategy<T> = serializersModule.serializer<T>(),
    writerSchema: Schema = schema(deserializer.descriptor),
): T {
    return decodeWithApacheDecoder(
        writerSchema,
        deserializer,
        DecoderFactory.get().directBinaryDecoder(ByteBufferInputStream(listOf(input)), null)
    )
}