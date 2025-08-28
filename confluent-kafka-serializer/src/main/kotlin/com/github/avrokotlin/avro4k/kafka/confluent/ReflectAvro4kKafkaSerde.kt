package com.github.avrokotlin.avro4k.kafka.confluent

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
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
 * To have a specific type to be serialized and deserialized known at compile time, use [SpecificAvro4kKafkaSerde].
 * To serialize and deserialize any type but without using the concrete class for enums and records, use [GenericAvro4kKafkaSerde].
 *
 * @see GenericAvro4kKafkaSerde
 * @see SpecificAvro4kKafkaSerde
 */
public fun ReflectAvro4kKafkaSerde(avro: Avro): ReflectAvro4kKafkaSerde =
    ReflectAvro4kKafkaSerde(
        ReflectAvro4kKafkaSerializer(avro),
        ReflectAvro4kKafkaDeserializer(avro)
    )

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
 * To have a specific type to be serialized and deserialized known at compile time, use [SpecificAvro4kKafkaSerde].
 * To serialize and deserialize any type but without using the concrete class for enums and records, use [GenericAvro4kKafkaSerde].
 *
 * @see GenericAvro4kKafkaSerde
 * @see SpecificAvro4kKafkaSerde
 */
@ExperimentalAvro4kApi
public open class ReflectAvro4kKafkaSerde(
    serializer: ReflectAvro4kKafkaSerializer,
    deserializer: ReflectAvro4kKafkaDeserializer,
) : WrapperSerde<Any>(serializer, deserializer) {
    public constructor() : this(ReflectAvro4kKafkaSerializer(), ReflectAvro4kKafkaDeserializer())

    /**
     * Create a configured instance of [ReflectAvro4kKafkaSerde].
     * You need to pass at least a non-null [schemaRegistry], or the `schema.registry.url` entry in [props].
     *
     * @see ReflectAvro4kKafkaSerde
     */
    public constructor(
        isKey: Boolean,
        avro: Avro = Avro,
        props: Map<String, Any?> = emptyMap(),
        schemaRegistry: SchemaRegistryClient? = null,
    ) : this(
        ReflectAvro4kKafkaSerializer(isKey, avro, props, schemaRegistry),
        ReflectAvro4kKafkaDeserializer(isKey, avro, props, schemaRegistry)
    )
}

@ExperimentalAvro4kApi
public class ReflectAvro4kKafkaSerializer(avro: Avro = Avro) : AbstractAvro4kKafkaSerializer<Any>(avro.withAnyKSerializer(ReflectKSerializer())) {
    public constructor(
        isKey: Boolean,
        avro: Avro = Avro,
        props: Map<String, Any?> = emptyMap(),
        schemaRegistry: SchemaRegistryClient? = null,
    ) : this(avro) {
        initialize(schemaRegistry, props, isKey)
    }

    override val serializer: SerializationStrategy<Any> = ReflectKSerializer()
}

@ExperimentalAvro4kApi
public class ReflectAvro4kKafkaDeserializer(avro: Avro = Avro) : AbstractAvro4kKafkaDeserializer<Any>(avro.withAnyKSerializer(ReflectKSerializer())) {
    /**
     * Create a configured instance of [ReflectAvro4kKafkaDeserializer].
     * You need to pass at least a non-null [schemaRegistry], or the `schema.registry.url` entry in [props].
     */
    public constructor(
        isKey: Boolean,
        avro: Avro = Avro,
        props: Map<String, Any?> = emptyMap(),
        schemaRegistry: SchemaRegistryClient? = null,
    ) : this(avro) {
        initialize(schemaRegistry, props, isKey)
    }

    override val deserializer: DeserializationStrategy<Any> = ReflectKSerializer()
}