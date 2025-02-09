package com.github.avrokotlin.avro4k

import com.github.avrokotlin.avro4k.internal.decodeWithApacheDecoder
import com.github.avrokotlin.avro4k.internal.decoder.direct.KotlinxIoDecoder
import com.github.avrokotlin.avro4k.internal.encodeWithApacheEncoder
import com.github.avrokotlin.avro4k.internal.encoder.direct.KotlinxIoEncoder
import kotlinx.io.Sink
import kotlinx.io.Source
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.serializer
import org.apache.avro.Schema

@ExperimentalSerializationApi
public fun <T> Avro.encodeToSink(
    writerSchema: Schema,
    serializer: SerializationStrategy<T>,
    value: T,
    sink: Sink,
) {
    val binaryEncoder = KotlinxIoEncoder(sink)
    encodeWithApacheEncoder(writerSchema, serializer, value, binaryEncoder)
    binaryEncoder.flush()
}

@ExperimentalSerializationApi
public inline fun <reified T> Avro.encodeToSink(
    value: T,
    sink: Sink,
) {
    val serializer = serializersModule.serializer<T>()
    encodeToSink(schema(serializer), serializer, value, sink)
}

@ExperimentalSerializationApi
public inline fun <reified T> Avro.encodeToSink(
    writerSchema: Schema,
    value: T,
    sink: Sink,
) {
    val serializer = serializersModule.serializer<T>()
    encodeToSink(writerSchema, serializer, value, sink)
}

@ExperimentalSerializationApi
public fun <T> Avro.decodeFromSource(
    writerSchema: Schema,
    deserializer: DeserializationStrategy<T>,
    source: Source,
): T {
    return decodeWithApacheDecoder(
        writerSchema,
        deserializer,
        KotlinxIoDecoder(source)
    )
}

@ExperimentalSerializationApi
public inline fun <reified T> Avro.decodeFromSource(source: Source): T {
    val serializer = serializersModule.serializer<T>()
    return decodeFromSource(schema(serializer.descriptor), serializer, source)
}

@ExperimentalSerializationApi
public inline fun <reified T> Avro.decodeFromSource(
    writerSchema: Schema,
    source: Source,
): T {
    val serializer = serializersModule.serializer<T>()
    return decodeFromSource(writerSchema, serializer, source)
}