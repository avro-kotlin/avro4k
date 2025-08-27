package com.github.avrokotlin.avro4k.kafka.confluent

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.serializer
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
 * To have a specific type to be serialized and deserialized known at compile time, use [SpecificAvro4kKafkaSerde].
 * To deserialize concrete java/kotlin classes for enums, records, fixed and other explicit class in the schema using `java-class` property, use [ReflectAvro4kKafkaSerde].
 *
 * @see ReflectAvro4kKafkaSerde
 * @see SpecificAvro4kKafkaSerde
 */
public fun GenericAvro4kKafkaSerde(avro: Avro): GenericAvro4kKafkaSerde = GenericAvro4kKafkaSerde(
    GenericAvro4kKafkaSerializer(avro),
    GenericAvro4kKafkaDeserializer(avro)
)

/**
 * A Serde that can serialize and deserialize any type supported by Avro4k, based on the confluent's schema registry.
 *
 * Enums are deserialized as [GenericEnumSymbol], records as [GenericRecord], and fixed as [GenericFixed].
 * Enums can be serialized as [GenericEnumSymbol] or as a string, records as [GenericRecord], and fixed as [GenericFixed] or as a byte array.
 *
 * If it is needed to configure the [Avro] instance but is restricted to an empty constructor (like as a default serde through properties in kafka streams which is instantiated through reflection),
 * you can extend this class and provide your own [Avro] instance, or handle the configuration by overriding the [configure] method.
 *
 * To have a specific type to be serialized and deserialized known at compile time, use [SpecificAvro4kKafkaSerde].
 * To deserialize concrete java/kotlin classes for enums, records, fixed and other explicit class in the schema using `java-class` property, use [ReflectAvro4kKafkaSerde].
 *
 * @see ReflectAvro4kKafkaSerde
 * @see SpecificAvro4kKafkaSerde
 */
@ExperimentalAvro4kApi
public open class GenericAvro4kKafkaSerde(
    serializer: GenericAvro4kKafkaSerializer,
    deserializer: GenericAvro4kKafkaDeserializer,
) : WrapperSerde<Any>(serializer, deserializer) {
    public constructor() : this(GenericAvro4kKafkaSerializer(), GenericAvro4kKafkaDeserializer())

    /**
     * Create a configured instance of [GenericAvro4kKafkaSerde].
     * You need to pass at least a non-null [schemaRegistry], or the `schema.registry.url` entry in [props].
     */
    public constructor(
        isKey: Boolean,
        avro: Avro = Avro,
        props: Map<String, Any?> = emptyMap(),
        schemaRegistry: SchemaRegistryClient? = null
    ) : this(
        GenericAvro4kKafkaSerializer(isKey, avro, props, schemaRegistry),
        GenericAvro4kKafkaDeserializer(isKey, avro, props, schemaRegistry)
    )
}

@ExperimentalAvro4kApi
public class GenericAvro4kKafkaSerializer(avro: Avro = Avro) : AbstractAvro4kKafkaSerializer<Any>(avro.withAnyKSerializer(GenericKSerializer())) {
    /**
     * Create a configured instance of [GenericAvro4kKafkaSerializer].
     * You need to pass at least a non-null [schemaRegistry], or the `schema.registry.url` entry in [props].
     *
     * @see GenericAvro4kKafkaSerde
     */
    public constructor(
        isKey: Boolean,
        avro: Avro = Avro,
        props: Map<String, Any?> = emptyMap(),
        schemaRegistry: SchemaRegistryClient? = null
    ) : this(avro) {
        initialize(schemaRegistry, props, isKey)
    }

    override val serializer: SerializationStrategy<Any> = avro.serializersModule.serializer()
}

@ExperimentalAvro4kApi
public class GenericAvro4kKafkaDeserializer(avro: Avro = Avro) : AbstractAvro4kKafkaDeserializer<Any>(avro.withAnyKSerializer(GenericKSerializer())) {
    /**
     * Create a configured instance of [GenericAvro4kKafkaDeserializer].
     * You need to pass at least a non-null [schemaRegistry], or the `schema.registry.url` entry in [props].
     *
     * @see GenericAvro4kKafkaSerde
     */
    public constructor(
        isKey: Boolean,
        avro: Avro = Avro,
        props: Map<String, Any?> = emptyMap(),
        schemaRegistry: SchemaRegistryClient? = null
    ) : this(avro) {
        initialize(schemaRegistry, props, isKey)
    }

    override val deserializer: DeserializationStrategy<Any> = avro.serializersModule.serializer()
}
