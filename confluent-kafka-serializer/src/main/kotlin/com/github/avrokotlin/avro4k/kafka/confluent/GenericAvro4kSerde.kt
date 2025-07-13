package com.github.avrokotlin.avro4k.kafka.confluent

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.SerializationStrategy
import org.apache.avro.generic.GenericEnumSymbol
import org.apache.avro.generic.GenericFixed
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.Serdes.WrapperSerde


/**
 * A Serde that can serialize and deserialize any type supported by Avro4k, based on the confluent's schema registry.
 *
 * Enums are deserialized as [GenericEnumSymbol], records as [GenericRecord], and fixed as [GenericFixed].
 * Enums can be serialized as [GenericEnumSymbol] or as a string, records as [GenericRecord], and fixed as [GenericFixed] or as a byte array.
 *
 * If it is needed to configure the [Avro] instance but is restricted to an empty constructor (like as a default serde through properties in kafka streams which is instantiated through reflection),
 * you can extend this class and provide your own [Avro] instance, or handle the configuration by overriding the [configure] method.
 *
 * To have a specific type to be serialized and deserialized known at compile time, use [Avro4kKafkaSerde].
 * To serialize and deserialize any type but using the concrete class for enums, records, fixed and other explicit class in the schema properties, use [ReflectAvro4kKafkaSerde].
 */
@ExperimentalAvro4kApi
public open class GenericAvro4kKafkaSerde(avro: Avro = Avro) :
    WrapperSerde<Any>(GenericAvro4kKafkaSerializer(avro), GenericAvro4kKafkaDeserializer(avro))

@ExperimentalAvro4kApi
public class GenericAvro4kKafkaSerializer(avro: Avro = Avro) : AbstractAvro4kKafkaSerializer<Any>(avro) {
    override val serializer: SerializationStrategy<Any>
        get() = GenericAnyKSerializer
}

@ExperimentalAvro4kApi
public class GenericAvro4kKafkaDeserializer(avro: Avro = Avro) : AbstractAvro4kKafkaDeserializer<Any>(avro) {
    override val deserializer: DeserializationStrategy<Any>
        get() = GenericAnyKSerializer
}
