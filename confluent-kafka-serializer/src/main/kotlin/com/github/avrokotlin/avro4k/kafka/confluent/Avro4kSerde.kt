package com.github.avrokotlin.avro4k.kafka.confluent

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.descriptors.nonNullOriginal
import kotlinx.serialization.serializer
import org.apache.avro.Schema
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes


/**
 * A Serde that can serialize and deserialize a specific type known at compile time supported by Avro4k, based on the confluent's schema registry.
 * The given type must be annotated with @Serializable or contextually accessible through the avro's serializers module.
 *
 * This is not a concrete Serde type as it is not meant to be generic and instantiated without a specific type.
 *
 * To allow serializing and deserializing any type supported by Avro4k, use [ReflectAvro4kKafkaSerde].
 * To serialize and deserialize any type but without using the concrete class for enums and records, use [GenericAvro4kKafkaSerde].
 */
@ExperimentalAvro4kApi
public inline fun <reified T> Avro4kSerde(avro: Avro = Avro): Serde<T> {
    val kSerializer = avro.serializersModule.serializer<T>()
    val serializer = Avro4kSerializer(kSerializer, avro)
    val deserializer = Avro4kDeserializer(kSerializer, avro)
    return Serdes.serdeFrom(serializer, deserializer)
}

@ExperimentalAvro4kApi
public inline fun <reified T> Avro4kSerializer(avro: Avro = Avro): Avro4kSerializer<T> =
    Avro4kSerializer(avro.serializersModule.serializer<T>(), avro)

@ExperimentalAvro4kApi
public inline fun <reified T> Avro4kDeserializer(avro: Avro = Avro): Avro4kDeserializer<T> =
    Avro4kDeserializer(avro.serializersModule.serializer<T>(), avro)

@ExperimentalAvro4kApi
public class Avro4kSerializer<T>(
    private val kSerializer: SerializationStrategy<T>,
    avro: Avro = Avro,
) : AbstractAvro4kSerializer<T>(avro) {
    private val schema = avro.schema(kSerializer.descriptor.nonNullOriginal)

    override fun getSerializer(data: T): SerializationStrategy<T> = kSerializer

    override fun getSchema(data: T): Schema = schema
}

@ExperimentalAvro4kApi
public class Avro4kDeserializer<T>(
    private val kSerializer: DeserializationStrategy<T>,
    avro: Avro = Avro,
) : AbstractAvro4kDeserializer<T>(avro) {
    override fun getDeserializer(writerSchema: Schema): DeserializationStrategy<T> = kSerializer
}