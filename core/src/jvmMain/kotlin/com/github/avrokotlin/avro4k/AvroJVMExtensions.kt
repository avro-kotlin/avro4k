package com.github.avrokotlin.avro4k

import kotlinx.io.asSink
import kotlinx.io.asSource
import kotlinx.io.buffered
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.SerializationStrategy
import org.apache.avro.Schema
import java.io.InputStream
import java.io.OutputStream

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