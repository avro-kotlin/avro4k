package com.github.avrokotlin.avro4k

import com.github.avrokotlin.avro4k.internal.decoder.generic.AvroValueGenericDecoder
import com.github.avrokotlin.avro4k.internal.encoder.generic.AvroValueGenericEncoder
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.serializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericContainer

private const val DEPRECATION_MSG =
    "The GenericData serialization has been deprecated prior to the new confluent-kafka-serializer module. Please file an issue if you still need GenericData serialization without a confluent schema registry"

@Deprecated(message = DEPRECATION_MSG, level = DeprecationLevel.WARNING)
@ExperimentalAvro4kApi
public fun <T> Avro.encodeToGenericData(
    writerSchema: Schema,
    serializer: SerializationStrategy<T>,
    value: T,
): Any? {
    var result: Any? = null
    AvroValueGenericEncoder(this, writerSchema) {
        result = it
    }.encodeSerializableValue(serializer, value)
    return result
}

@Deprecated(message = DEPRECATION_MSG, level = DeprecationLevel.WARNING)
@ExperimentalAvro4kApi
public inline fun <reified T> Avro.encodeToGenericData(value: T): Any? {
    val serializer = serializersModule.serializer<T>()
    @Suppress("DEPRECATION")
    return encodeToGenericData(schema(serializer), serializer, value)
}

@Deprecated(message = DEPRECATION_MSG, level = DeprecationLevel.WARNING)
@ExperimentalAvro4kApi
public inline fun <reified T> Avro.encodeToGenericData(
    writerSchema: Schema,
    value: T,
): Any? {
    val serializer = serializersModule.serializer<T>()
    @Suppress("DEPRECATION")
    return encodeToGenericData(writerSchema, serializer, value)
}

@Deprecated(message = DEPRECATION_MSG, level = DeprecationLevel.WARNING)
@ExperimentalAvro4kApi
public fun <T> Avro.decodeFromGenericData(
    writerSchema: Schema,
    deserializer: DeserializationStrategy<T>,
    value: Any?,
): T {
    return AvroValueGenericDecoder(this, value, writerSchema)
        .decodeSerializableValue(deserializer)
}

@Deprecated(message = DEPRECATION_MSG, level = DeprecationLevel.WARNING)
@ExperimentalAvro4kApi
public inline fun <reified T> Avro.decodeFromGenericData(
    writerSchema: Schema,
    value: Any?,
): T {
    val deserializer = serializersModule.serializer<T>()
    @Suppress("DEPRECATION")
    return decodeFromGenericData(writerSchema, deserializer, value)
}

@Deprecated(message = DEPRECATION_MSG, level = DeprecationLevel.WARNING)
@ExperimentalAvro4kApi
public inline fun <reified T> Avro.decodeFromGenericData(value: GenericContainer?): T? {
    if (value == null) return null
    val deserializer = serializersModule.serializer<T>()
    @Suppress("DEPRECATION")
    return decodeFromGenericData(value.schema, deserializer, value)
}