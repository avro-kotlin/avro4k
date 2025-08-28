package com.github.avrokotlin.avro4k.kafka.confluent

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.internal.aliases
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.descriptors.nonNullOriginal
import kotlinx.serialization.serializer
import org.apache.avro.Schema
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes.WrapperSerde
import kotlin.reflect.KClass

/**
 * A Serde that can serialize and deserialize a specific serializable type known at compile time supported by Avro4k, based on the confluent's schema registry.
 * The given type must be annotated with @Serializable or contextually accessible through the avro's serializers module.
 *
 * This is not a concrete Serde type as it is not meant to be generic and instantiated without a specific type.
 *
 * To allow serializing and deserializing any type supported by Avro4k without using concrete java/kotlin classes, use [GenericAvro4kKafkaSerde].
 * To deserialize concrete java/kotlin classes for enums, records, fixed and other explicit class in the schema using `java-class` property, use [ReflectAvro4kKafkaSerde].
 *
 * @see GenericAvro4kKafkaSerde
 * @see ReflectAvro4kKafkaSerde
 */
@Suppress("FunctionName")
@ExperimentalAvro4kApi
public inline fun <reified T : Any> SpecificAvro4kKafkaSerde(avro: Avro = Avro): Serde<T> {
    val kSerializer = avro.serializersModule.serializer<T>()
    return SpecificAvro4kKafkaSerde(kSerializer, avro)
}

@Suppress("FunctionName")
@ExperimentalAvro4kApi
public inline fun <reified T : Any> SpecificAvro4kKafkaSerde(
    isKey: Boolean,
    avro: Avro = Avro,
    props: Map<String, Any?> = emptyMap(),
    schemaRegistry: SchemaRegistryClient? = null,
): Serde<T> {
    val kSerializer = avro.serializersModule.serializer<T>()
    return SpecificAvro4kKafkaSerde(kSerializer, isKey, avro, props, schemaRegistry)
}

@ExperimentalAvro4kApi
public open class SpecificAvro4kKafkaSerde<T : Any>(
    serializer: SpecificAvro4kKafkaSerializer<T>,
    deserializer: SpecificAvro4kKafkaDeserializer<T>,
) : WrapperSerde<T>(serializer, deserializer) {
    public constructor(
        kSerializer: KSerializer<T>,
        avro: Avro = Avro,
    ) : this(
        SpecificAvro4kKafkaSerializer(kSerializer, avro),
        SpecificAvro4kKafkaDeserializer(kSerializer, avro)
    )

    /**
     * Create a configured instance of [SpecificAvro4kKafkaSerde].
     * You need to pass at least a non-null [schemaRegistry], or the `schema.registry.url` entry in [props].
     *
     * @see SpecificAvro4kKafkaSerde
     */
    public constructor(
        kSerializer: KSerializer<T>,
        isKey: Boolean,
        avro: Avro = Avro,
        props: Map<String, Any?> = emptyMap(),
        schemaRegistry: SchemaRegistryClient? = null,
    ) : this(
        SpecificAvro4kKafkaSerializer(kSerializer, isKey, avro, props, schemaRegistry),
        SpecificAvro4kKafkaDeserializer(kSerializer, isKey, avro, props, schemaRegistry)
    )
}

@ExperimentalAvro4kApi
public inline fun <reified T : Any> SpecificAvro4kKafkaSerializer(avro: Avro = Avro): SpecificAvro4kKafkaSerializer<T> =
    SpecificAvro4kKafkaSerializer(avro.serializersModule.serializer<T>(), avro)

/**
 * Create a configured instance of [SpecificAvro4kKafkaSerializer].
 * You need to pass at least a non-null [schemaRegistry], or the `schema.registry.url` entry in [props].
 */
@ExperimentalAvro4kApi
public inline fun <reified T : Any> SpecificAvro4kKafkaSerializer(
    isKey: Boolean,
    avro: Avro = Avro,
    props: Map<String, Any?> = emptyMap(),
    schemaRegistry: SchemaRegistryClient? = null,
): SpecificAvro4kKafkaSerializer<T> =
    SpecificAvro4kKafkaSerializer(avro.serializersModule.serializer<T>(), isKey, avro, props, schemaRegistry)

@ExperimentalAvro4kApi
public class SpecificAvro4kKafkaSerializer<T : Any>(
    serializer: SerializationStrategy<T> = ReflectKSerializer(),
    avro: Avro = Avro,
) : AbstractAvro4kKafkaSerializer<T>(avro.withAnyKSerializer(ReflectKSerializer())) {
    override var serializer: SerializationStrategy<T> private set

    init {
        this.serializer = serializer
    }

    /**
     * Create a configured instance of [SpecificAvro4kKafkaSerializer].
     * You need to pass at least a non-null [schemaRegistry], or the `schema.registry.url` entry in [props].
     */
    public constructor(
        serializer: SerializationStrategy<T> = ReflectKSerializer(),
        isKey: Boolean,
        avro: Avro = Avro,
        props: Map<String, Any?> = emptyMap(),
        schemaRegistry: SchemaRegistryClient? = null,
    ) : this(serializer, avro) {
        initialize(schemaRegistry, props, isKey)
    }

    @OptIn(ExperimentalSerializationApi::class)
    private val schema = avro.schema(serializer.descriptor.nonNullOriginal)

    override fun getSchema(value: T): Schema = schema
}

@ExperimentalAvro4kApi
public inline fun <reified T : Any> SpecificAvro4kKafkaDeserializer(avro: Avro = Avro): SpecificAvro4kKafkaDeserializer<T> =
    SpecificAvro4kKafkaDeserializer(avro.serializersModule.serializer<T>(), avro)

/**
 * Create a configured instance of [SpecificAvro4kKafkaDeserializer].
 * You need to pass at least a non-null [schemaRegistry], or the `schema.registry.url` entry in [props].
 *
 * If the `specific.avro.key.type` / `specific.avro.value.type` entry in [props] is set, it will override the [T]'s deserializer.
 */
@ExperimentalAvro4kApi
public inline fun <reified T : Any> SpecificAvro4kKafkaDeserializer(
    isKey: Boolean,
    avro: Avro = Avro,
    props: Map<String, Any?> = emptyMap(),
    schemaRegistry: SchemaRegistryClient? = null,
): SpecificAvro4kKafkaDeserializer<T> =
    SpecificAvro4kKafkaDeserializer(avro.serializersModule.serializer<T>(), isKey, avro, props, schemaRegistry)

/**
 * This deserializer will only return instances of [T], or throw an exception if the data cannot be deserialized to that type.
 *
 * @see SpecificAvro4kKafkaSerde
 */
@ExperimentalAvro4kApi
public class SpecificAvro4kKafkaDeserializer<T : Any>(
    deserializer: DeserializationStrategy<T>? = null,
    avro: Avro = Avro,
) : AbstractAvro4kKafkaDeserializer<T>(avro.withAnyKSerializer(ReflectKSerializer())) {
    override lateinit var deserializer: DeserializationStrategy<T> private set

    init {
        if (deserializer != null) {
            this.deserializer = deserializer
        }
    }

    /**
     * Create a configured instance of [SpecificAvro4kKafkaDeserializer].
     * You need to pass at least:
     * - a non-null [schemaRegistry], or the `schema.registry.url` entry in [props]
     * - a non-null [deserializer], or the `specific.avro.key.type` / `specific.avro.value.type` entry in [props] depending on [isKey]. The [props] priors over a non-null [deserializer].
     */
    public constructor(
        deserializer: DeserializationStrategy<T>? = null,
        isKey: Boolean,
        avro: Avro = Avro,
        props: Map<String, Any?> = emptyMap(),
        schemaRegistry: SchemaRegistryClient? = null,
    ) : this(deserializer, avro) {
        initialize(schemaRegistry, props, isKey)
    }

    @OptIn(ExperimentalSerializationApi::class)
    override fun configure(config: KafkaAvroDeserializerConfig) {
        val specificAvroClassLookupKey =
            when (isKey) {
                true -> KafkaAvroDeserializerConfig.SPECIFIC_AVRO_KEY_TYPE_CONFIG
                false -> KafkaAvroDeserializerConfig.SPECIFIC_AVRO_VALUE_TYPE_CONFIG
            }

        config.getString(specificAvroClassLookupKey)?.let { typeName ->
            val foundKSerializer =
                findKSerializerForClassName(typeName)
                    ?: findDeserializationStrategyInSerializersModule(typeName)
                    ?: throw ConfigException(
                        specificAvroClassLookupKey,
                        typeName,
                        "The configured type name was not found "
                    )
            @Suppress("UNCHECKED_CAST")
            deserializer = foundKSerializer as DeserializationStrategy<T>
        }

        super.configure(config)
    }

    @OptIn(ExperimentalSerializationApi::class)
    private fun findKSerializerForClassName(serialName: String): DeserializationStrategy<*>? =
        runCatching { Class.forName(serialName).kotlin }.getOrNull()?.let { avro.serializersModule.serializer(it, it.typeParameters.map { ReflectKSerializer() }, false) }

    @OptIn(ExperimentalSerializationApi::class)
    private fun findDeserializationStrategyInSerializersModule(serialName: String): DeserializationStrategy<*>? {
        return KSerializerFinder(serialName).let {
            avro.serializersModule.dumpTo(it)
            it.found
        }
    }
}

private class KSerializerFinder(
    private val serialName: String,
) : SimpleSerializersModuleCollector {
    var found: DeserializationStrategy<*>? = null

    @OptIn(ExperimentalSerializationApi::class)
    private fun setFoundKSerializer(serializer: DeserializationStrategy<*>) {
        if (found != null) return
        val descriptor = serializer.descriptor.nonNullOriginal
        if (descriptor.serialName == serialName || serialName in descriptor.aliases) {
            found = serializer
        }
    }

    override fun <T : Any> contextual(kClass: KClass<T>, serializer: KSerializer<T>) =
        setFoundKSerializer(serializer)

    override fun <T : Any> contextual(kClass: KClass<T>, provider: (typeArgumentsSerializers: List<KSerializer<*>>) -> KSerializer<*>) =
        setFoundKSerializer(provider(kClass.typeParameters.map { ReflectKSerializer() }))

    override fun <Base : Any, Sub : Base> polymorphic(baseClass: KClass<Base>, actualClass: KClass<Sub>, actualSerializer: KSerializer<Sub>) =
        setFoundKSerializer(actualSerializer)

    override fun <Base : Any> polymorphicDefaultDeserializer(baseClass: KClass<Base>, defaultDeserializerProvider: (className: String?) -> DeserializationStrategy<Base>?) {
        defaultDeserializerProvider(serialName)?.let { setFoundKSerializer(it) }
    }
}