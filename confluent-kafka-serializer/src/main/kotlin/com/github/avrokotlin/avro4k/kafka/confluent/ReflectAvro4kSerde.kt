package com.github.avrokotlin.avro4k.kafka.confluent

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.SerializationStrategy
import org.apache.kafka.common.serialization.Serdes.WrapperSerde


/**
 * A Serde that can serialize and deserialize any type supported by Avro4k, based on the confluent's schema registry.
 *
 * Enums and records are deserialized to their concrete class, by using the schema's full name as the class name.
 * It also requires to have the enums and records serializers statically accessible (their type is annotated with @Serializable), or
 * contextually accessible through the avro's serializers module.
 *
 * If it is needed to configure the [Avro] instance but is restricted to an empty constructor (like as a default serde through properties in kafka streams which is instantiated through reflection),
 * you can extend this class and provide your own [Avro] instance, or handle the configuration by overriding the [configure] method.
 *
 * To have a specific type to be serialized and deserialized known at compile time, use [Avro4kKafkaSerde].
 * To serialize and deserialize any type but without using the concrete class for enums and records, use [GenericAvro4kKafkaSerde].
 */
@ExperimentalAvro4kApi
public open class ReflectAvro4kKafkaSerde(avro: Avro = Avro) : WrapperSerde<Any>(ReflectAvro4kKafkaSerializer(avro), ReflectAvro4kKafkaDeserializer(avro))

@ExperimentalAvro4kApi
public class ReflectAvro4kKafkaSerializer(avro: Avro = Avro) : AbstractAvro4kKafkaSerializer<Any>(avro) {
    override val serializer: SerializationStrategy<Any>
        get() = AvroReflectKSerializer
}

@ExperimentalAvro4kApi
public class ReflectAvro4kKafkaDeserializer(avro: Avro = Avro) : AbstractAvro4kKafkaDeserializer<Any>(avro) {
    override val deserializer: DeserializationStrategy<Any>
        get() = AvroReflectKSerializer
}
