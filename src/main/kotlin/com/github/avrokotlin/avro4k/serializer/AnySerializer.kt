package com.github.avrokotlin.avro4k.serializer

import com.github.avrokotlin.avro4k.AvroConfiguration
import com.github.avrokotlin.avro4k.InternalAvro4kApi
import com.github.avrokotlin.avro4k.internal.decoder.direct.AbstractAvroDirectDecoder
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationException
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.builtins.ByteArraySerializer
import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.builtins.MapSerializer
import kotlinx.serialization.builtins.nullable
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.SerialKind
import kotlinx.serialization.descriptors.buildSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.contextual
import kotlinx.serialization.serializer
import kotlinx.serialization.serializerOrNull
import org.apache.avro.Schema
import java.util.WeakHashMap

internal val AnyTypeSerializersModule: SerializersModule
    get() =
        SerializersModule {
            contextual(AnySerializer())
        }

/**
 * This serializer handles [Any] type, for both encoding and decoding.
 *
 * At encoding, it barely gets the [KSerializer] from the value's type (see [inferSerializationStrategyFromNonSerializableType] for customization).
 *
 * At decoding, it delegates the process to the inferred [KSerializer] from the [Schema.type] (example: [Schema.Type.BOOLEAN] is handled by the [Boolean]'s serializer).
 * For more customization about named types, see [resolveFixedDeserializationStrategy], [resolveEnumDeserializationStrategy], and [resolveRecordDeserializationStrategy].
 */
@InternalAvro4kApi
public open class AnySerializer : KSerializer<Any> {
    // No need to use a WeakHashMap with class keys
    private val encodingCache = HashMap<Class<out Any>, SerializationStrategy<Any>>()
    private val decodingCache = WeakHashMap<Schema, DeserializationStrategy<Any>>()

    /**
     * Provides the nullable version of this [AnySerializer].
     *
     * Prefer use this instead of [KSerializer.nullable] extension to prevent useless allocations.
     */
    public val nullable: KSerializer<Any?> = nullableSerializer(this)

    /**
     * Uses a method to provide a nullable serializer for the given [serializer].
     * Helps to not use the [AnySerializer.nullable], so it does not loop on itself.
     */
    private fun <T : Any> nullableSerializer(serializer: KSerializer<T>): KSerializer<T?> {
        return serializer.nullable
    }

    @OptIn(InternalSerializationApi::class)
    override val descriptor: SerialDescriptor
        get() = buildSerialDescriptor(Any::class.qualifiedName!!, SerialKind.CONTEXTUAL)

    override fun serialize(encoder: Encoder, value: Any) {
        val serializer = encodingCache.computeIfAbsent(value::class.java) { encoder.serializersModule.inferSerializationStrategy(it) }
        encoder.encodeSerializableValue(serializer, value)
    }

    @Suppress("UNCHECKED_CAST")
    private fun SerializersModule.inferSerializationStrategy(type: Class<out Any>): SerializationStrategy<Any> {
        // First, try to find the serializer in the serializers module as-is. Here generic types are resolved if possible
        serializerOrNull(type)?.let {
            return@inferSerializationStrategy it
        }
        // Then, try to find the serializer for the type with its type parameters as nullable Any. Any parameterized types will be handled as nullable Any.
        // This handles all the collections, maps, kotlin pair, triple, etc.
        @OptIn(ExperimentalSerializationApi::class)
        runCatching { serializer(type.kotlin, type.typeParameters.map { this@AnySerializer.nullable }, false) }.getOrNull()?.let {
            return@inferSerializationStrategy it as KSerializer<Any>
        }

        if (Collection::class.java.isAssignableFrom(type)) {
            return ListSerializer(this@AnySerializer.nullable) as KSerializer<Any>
        }

        if (Map::class.java.isAssignableFrom(type)) {
            return MapSerializer(this@AnySerializer.nullable, this@AnySerializer.nullable) as KSerializer<Any>
        }

        return inferSerializationStrategyFromNonSerializableType(type) as SerializationStrategy<Any>?
            ?: throw SerializationException("Cannot find serializer for type $type. Please register a serializer for this type in the serializers module.")
    }

    /**
     * Called when avro4k did not find the corresponding [type]'s serializer in the [com.github.avrokotlin.avro4k.Avro.serializersModule].
     *
     * Generally useful when you don't know the exact type at runtime, but the interface.
     *
     * Results are cached, so expect only one call per [type].
     *
     * @return the serializer to use for the given [type], or `null` if none, which will raise a [SerializationException].
     */
    @InternalAvro4kApi
    protected open fun SerializersModule.inferSerializationStrategyFromNonSerializableType(type: Class<out Any>): SerializationStrategy<*>? {
        return null
    }

    override fun deserialize(decoder: Decoder): Any {
        decoder as AbstractAvroDirectDecoder
        val serializer =
            decodingCache.computeIfAbsent(decoder.currentWriterSchema) {
                decoder.serializersModule.resolveDeserializationStrategy(decoder.currentWriterSchema, decoder.avro.configuration)
            }
        return decoder.decodeSerializableValue(serializer)
    }

    private fun SerializersModule.resolveDeserializationStrategy(writerSchema: Schema, configuration: AvroConfiguration): DeserializationStrategy<Any> {
        preResolveDeserializationStrategy(writerSchema)?.let { return it }
        // Let's try to use the logical type's serializer if any
        writerSchema.logicalType?.name?.let { configuration.logicalTypes[it] }?.let { return it }
        // Else infer the serializer from the schema type
        @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
        return when (writerSchema.type) {
            Schema.Type.BOOLEAN -> Boolean.serializer()
            Schema.Type.INT -> Int.serializer()
            Schema.Type.LONG -> Long.serializer()
            Schema.Type.FLOAT -> Float.serializer()
            Schema.Type.DOUBLE -> Double.serializer()
            Schema.Type.STRING -> String.serializer()
            Schema.Type.BYTES -> ByteArraySerializer()

            // We cannot recursively resolve the serializer, as we don't want to manage unions which can be different for each item
            Schema.Type.ARRAY -> ListSerializer(this@AnySerializer.nullable)
            Schema.Type.MAP -> MapSerializer(String.serializer(), this@AnySerializer.nullable)

            // Named types can be decoded differently depending on the needs
            Schema.Type.FIXED -> resolveFixedDeserializationStrategy(writerSchema)
            Schema.Type.ENUM -> resolveEnumDeserializationStrategy(writerSchema)
            Schema.Type.RECORD -> resolveRecordDeserializationStrategy(writerSchema)

            // Having an union here is an avro4k error, or a wrong usage of this serializer
            Schema.Type.UNION -> throw UnsupportedOperationException(
                "Do not directly decode values with ${this::class}. " +
                    "Please use Avro.decode methods instead. " +
                    "(error: had to get deserializer of union, which should be resolved by internal decoders before calling deserializers)"
            )

            // Having a null here is an avro4k error, or a wrong usage of this serializer
            Schema.Type.NULL -> throw UnsupportedOperationException(
                "Do not directly decode values with ${this::class}. " +
                    "Please use Avro.decode methods instead. " +
                    "(error: had to get deserializer of null, which should be handled by internal decoders)"
            )
        }
    }

    /**
     * Allows to provide a serializer for the given [writerSchema] before checking its [Schema.type] or its [Schema.logicalType].
     * Useful to provide a custom serializer depending on a custom property.
     *
     * @param writerSchema the schema from which the value is being decoded
     * @return the serializer to be used, or `null` (the default) if no serializer is found.
     */
    @InternalAvro4kApi
    protected open fun SerializersModule.preResolveDeserializationStrategy(writerSchema: Schema): DeserializationStrategy<Any>? {
        return null
    }

    /**
     * Provides the serializer for the given enum schema.
     * [resolveEnumDeserializationStrategy] won't be called if the [writerSchema] has its [Schema.logicalType]'s name found in [AvroConfiguration.logicalTypes].
     *
     * By default, an enum value is decoded as a [String].
     *
     * Results are cached, so expect only one call per [writerSchema].
     *
     * @param writerSchema the schema always being of the type [Schema.Type.ENUM]
     * @return the serializer to be used for enum types when decoding
     */
    @InternalAvro4kApi
    protected open fun SerializersModule.resolveEnumDeserializationStrategy(writerSchema: Schema): DeserializationStrategy<Any> {
        return String.serializer()
    }

    /**
     * Provides the serializer for the given fixed schema.
     * [resolveFixedDeserializationStrategy] won't be called if the [writerSchema] has its [Schema.logicalType]'s name found in [AvroConfiguration.logicalTypes].
     *
     * By default, a fixed value is decoded as a [ByteArray].
     *
     * Results are cached, so expect only one call per [writerSchema].
     *
     * @param writerSchema the schema always being of the type [Schema.Type.FIXED]
     * @return the serializer to be used for fixed types when decoding
     */
    @InternalAvro4kApi
    protected open fun SerializersModule.resolveFixedDeserializationStrategy(writerSchema: Schema): DeserializationStrategy<Any> {
        return ByteArraySerializer()
    }

    /**
     * Provides the serializer for the given record schema.
     * [resolveRecordDeserializationStrategy] won't be called if the [writerSchema] has its [Schema.logicalType]'s name found in [AvroConfiguration.logicalTypes].
     *
     * By default, a record is decoded as a [Map], where the keys are field names as [String] and the values are field values.
     *
     * Important notes:
     * For recursively resolve a subtype, do provide a nullable [AnySerializer] to allow resolving the schema at runtime depending on the [writerSchema].
     * Also, results are cached, so expect only one call per [writerSchema].
     *
     * @param writerSchema the schema always being of the type [Schema.Type.RECORD]
     * @return the serializer to be used for record types when decoding
     */
    @InternalAvro4kApi
    protected open fun SerializersModule.resolveRecordDeserializationStrategy(writerSchema: Schema): DeserializationStrategy<Any> {
        return MapSerializer(String.serializer(), this@AnySerializer.nullable)
    }
}