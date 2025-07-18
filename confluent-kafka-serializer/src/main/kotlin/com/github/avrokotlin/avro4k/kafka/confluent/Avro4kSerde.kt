package com.github.avrokotlin.avro4k.kafka.confluent

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.descriptors.nonNullOriginal
import kotlinx.serialization.serializer
import org.apache.avro.Schema
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes


/**
 * A Serde that can serialize and deserialize a specific serializable type known at compile time supported by Avro4k, based on the confluent's schema registry.
 * The given type must be annotated with @Serializable or contextually accessible through the avro's serializers module.
 *
 * This is not a concrete Serde type as it is not meant to be generic and instantiated without a specific type.
 *
 * To allow serializing and deserializing any type supported by Avro4k, use [ReflectAvro4kKafkaSerde].
 * To serialize and deserialize any type but without using the concrete class for enums and records, use [GenericAvro4kKafkaSerde].
 */
@Suppress("FunctionName")
@ExperimentalAvro4kApi
public inline fun <reified T : Any> Avro4kKafkaSerde(avro: Avro = Avro): Serde<T> {
    val kSerializer = avro.serializersModule.serializer<T>()
    val serializer = Avro4kKafkaSerializer(kSerializer, avro)
    val deserializer = Avro4kKafkaDeserializer(kSerializer, avro)
    return Serdes.serdeFrom(serializer, deserializer)
}

@ExperimentalAvro4kApi
public inline fun <reified T : Any> Avro4kKafkaSerializer(avro: Avro = Avro): Avro4kKafkaSerializer<T> =
    Avro4kKafkaSerializer(avro.serializersModule.serializer<T>(), avro)

@ExperimentalAvro4kApi
public inline fun <reified T : Any> Avro4kKafkaDeserializer(avro: Avro = Avro): Avro4kKafkaDeserializer<T> =
    Avro4kKafkaDeserializer(avro.serializersModule.serializer<T>(), avro)

@ExperimentalAvro4kApi
public class Avro4kKafkaSerializer<T : Any>(
    override val serializer: SerializationStrategy<T>,
    avro: Avro = Avro,
) : AbstractAvro4kKafkaSerializer<T>(avro) {
    @OptIn(ExperimentalSerializationApi::class)
    private val schema = avro.schema(serializer.descriptor.nonNullOriginal)

    override fun getSchema(value: T): Schema = schema
}

@ExperimentalAvro4kApi
public class Avro4kKafkaDeserializer<T : Any>(
    override val deserializer: DeserializationStrategy<T>,
    avro: Avro = Avro,
) : AbstractAvro4kKafkaDeserializer<T>(avro)