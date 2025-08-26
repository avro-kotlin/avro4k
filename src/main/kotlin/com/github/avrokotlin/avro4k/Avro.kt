package com.github.avrokotlin.avro4k

import com.github.avrokotlin.avro4k.internal.Buffer
import com.github.avrokotlin.avro4k.internal.EnumResolver
import com.github.avrokotlin.avro4k.internal.PolymorphicResolver
import com.github.avrokotlin.avro4k.internal.RecordResolver
import com.github.avrokotlin.avro4k.internal.schema.ValueVisitor
import com.github.avrokotlin.avro4k.serializer.AnyTypeSerializersModule
import com.github.avrokotlin.avro4k.serializer.JavaStdLibSerializersModule
import com.github.avrokotlin.avro4k.serializer.JavaTimeSerializersModule
import kotlinx.io.Buffer
import kotlinx.io.readByteArray
import kotlinx.serialization.BinaryFormat
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationException
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.modules.EmptySerializersModule
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.overwriteWith
import kotlinx.serialization.modules.plus
import kotlinx.serialization.serializer
import org.apache.avro.Schema
import java.util.WeakHashMap

/**
 * The goal of this class is to serialize and deserialize in avro binary format.
 * That said, following the avro specification, each logical type is translated to a specific avro type and serialized as a specific byte sequence (strings in UTF8 bytes, int as zig-zag varint, etc).
 *
 * The main advantage of this library is that it allows you to serialize your data classes (and any other serializable type) to avro binary format, with the kotlin-style native serialization API provided by `kotlinx.serialization`.
 * This framework maintained by jetbrains provides a powerful framework and gradle plugin to generate serializers at compile time, just with a few [kotlinx.serialization.Serializable] anotations.
 *
 * @sample com.github.avrokotlin.avro4k.internal.Samples.customizeAvroInstance
 */
public sealed class Avro(
    public val configuration: AvroConfiguration,
    public final override val serializersModule: SerializersModule,
) : BinaryFormat {
    private val schemaCache: MutableMap<SerialDescriptor, Schema> = WeakHashMap()

    internal val recordResolver = RecordResolver(this)
    internal val polymorphicResolver = PolymorphicResolver(serializersModule)
    internal val enumResolver = EnumResolver()

    public companion object Default : Avro(
        AvroConfiguration(),
        JavaStdLibSerializersModule +
            JavaTimeSerializersModule +
            AnyTypeSerializersModule
    )

    /**
     * Infers the Avro schema for the given [descriptor].
     *
     * You may not need to call this method if you use the other encode/decode extension methods, as they will automatically generate the schema for you based on the serializer's descriptor.
     *
     * ## Types mapping
     * - Classes and objects are translated as Avro records
     * - Sealed classes are translated as UNION types
     * - abstract classes must have their subclasses registered in the [Avro.serializersModule] to be properly translated
     * - Enums are translated as Avro enums, taking into account the [AvroEnumDefault] annotation to set the default value
     * - value classes are inlined as their underlying type
     * - Primitive types such as [Boolean], [Int], [Long], [Float], [Double], and [String] are translated as their respective Avro types.
     * - [Byte] and [Short] are translated as [Int] in Avro.
     * - [UByte], [UShort], [UInt], and [ULong] are translated as their signed counterparts ([Int] and [Long]).
     * - [ByteArray] is translated as BYTES type.
     * - For the FIXED type, you can wrap a [ByteArray] in a value class, and annotate the underlying field with [AvroFixed]
     * - [Map] is translated as a MAP type, where the keys are always converted from/to [String] (only for the primitive types)
     * - Any other serializable [Iterable] is translated as an ARRAY type
     * - Nullable types are translated as UNION types, with the null type being the first type in the union
     * - [AvroDoc] annotation will be used to generate the Avro doc for a field or a named type (record, enum, fixed)
     * - [AvroAlias] annotation will be used to set one or more aliases for a field or a named type (record, enum, fixed)
     * - Annotate fields with [AvroStringable] to force the field type to be a string in the schema
     * - Annotate a field with [AvroDefault] to set a default value for the field in the schema. The default value must be compatible with the field type.
     * - Annotate a [java.math.BigDecimal] field with [AvroDecimal] to specify the scale and precision of the decimal avro logical type
     * - Annotate a field or a type with [AvroProp] to add a custom property to the generate type's schema
     *
     * ## Cache details
     * This method will cache the schema for the given descriptor, so subsequent calls with the same descriptor will return the cached schema.
     * The cache is weakly referenced, meaning that it will not prevent the descriptor from being garbage collected.
     * Also, the cache is not bounded, so calling this method with many different descriptors may lead to increased memory usage.
     *
     * ## Compatibility
     * Do not forget to annotate evolving types and field names with [AvroAlias] to ensure compatibility with existing schemas and the used writer schemas.
     *
     * @param descriptor the descriptor for which to generate the schema.
     * @return the Avro schema for the given descriptor.
     *
     * @see AvroConfiguration
     * @see AvroBuilder.serializersModule
     * @see com.github.avrokotlin.avro4k.serializer.AvroSerializer
     */
    public fun schema(descriptor: SerialDescriptor): Schema {
        return schemaCache.getOrPut(descriptor) {
            lateinit var output: Schema
            ValueVisitor(this) { output = it }.visitValue(descriptor)
            output
        }
    }

    /**
     * Encodes the given [value] to a [ByteArray] using the provided [writerSchema] and [serializer].
     *
     * You may prefer the other extension methods that infer the [serializer] and [writerSchema] based on a generic type [T].
     *
     * The given [serializer] must be "at least" compatible with the [writerSchema], meaning that any class field that is not present in the [writerSchema] will be ignored during encoding.
     * Avro4k also match the [AvroAlias] of the classes and fields, and matches the [writerSchema]'s aliases to maximize the chances of compatibility with existing schemas.
     * The schema can be generated using the [schema] method, or may be different, so that avro4k will adapt the value to the schema.
     *
     * ## Supported conversions
     * - int types [Byte], [Short], [Int], [Long], their unsigned counterparts, and [Char] can be encoded INT or LONG
     * - [Long] can be encoded to INT, but will fail if the value doesn't fit in the int range (same for unsigned types)
     * - [Float] can be encoded to DOUBLE
     * - [Double] can be encoded to FLOAT but will fail if the value doesn't fit in the float range
     * - [String] can be encoded to any numerical type if the string is parsable
     * - [String] can be encoded as UTF8 to BYTES and FIXED types, the later requiring the fixed size to match the actual string length
     * - [String] can be encoded as ENUM
     * - [Char] can be encoded as STRING
     * - Classes and objects are encoded as RECORD, taking into account both [AvroAlias] and the [writerSchema]'s aliases to maximize compatibility
     *
     * @see AvroConfiguration
     * @see AvroBuilder.serializersModule
     */
    public fun <T> encodeToByteArray(
        writerSchema: Schema,
        serializer: SerializationStrategy<T>,
        value: T,
    ): ByteArray {
        val buffer = Buffer()
        encodeToSink(writerSchema, serializer, value, buffer)
        return buffer.readByteArray()
    }

    /**
     * Decode the value from the given [bytes] to a value of type [T] assuming the data is respecting the given [writerSchema].
     * If during the decoding, the written data does not match the [writerSchema], you may end up to unexpected results or an exception.
     *
     * You may prefer the other extension methods that infer the [deserializer] and [writerSchema] based on a generic type [T].
     *
     * For any custom encoding or conversion of type [T] or its inner type, you may need to register a custom [kotlinx.serialization.KSerializer] in [Avro.serializersModule].
     *
     * @param writerSchema the schema to use for decoding the value.
     * @param deserializer the deserialization strategy to use for decoding the value. You may prefer the other extension methods without the deserializer parameter for convenience.
     * @param bytes the [ByteArray] to read the encoded value from.
     *
     * @throws SerializationException if not all bytes were consumed during deserialization, or if the data does not match the [writerSchema].
     *
     * @see AvroConfiguration
     * @see AvroBuilder.serializersModule
     */
    public fun <T> decodeFromByteArray(
        writerSchema: Schema,
        deserializer: DeserializationStrategy<T>,
        bytes: ByteArray,
    ): T {
        val source = Buffer(bytes)
        val result = decodeFromSource(writerSchema, deserializer, source)
        if (!source.exhausted()) {
            throw SerializationException("Not all bytes were consumed during deserialization")
        }
        return result
    }

    /**
     * Decode the value from the given [bytes] to a value of type [T] assuming the data is respecting the given [writerSchema].
     * If during the decoding, the written data does not match the [writerSchema], you may end up to unexpected results or an exception.
     *
     * You may prefer the other extension methods that infer the [serializer] based on a generic type [T].
     *
     * For any custom encoding or conversion of type [T] or its inner type, you may need to register a custom [kotlinx.serialization.KSerializer] in [Avro.serializersModule].
     *
     * ## Supported conversions
     * - LONG and INT types can be decoded into [Byte], [Short], [Int], [Long], their unsigned counterparts, and [Char] (if the value fits in the type range)
     * - FLOAT and DOUBLE types can be decoded into [Float] and [Double] (if the value fits in the type range)
     * - STRING can be decoded into any numerical type, as long as the string is parsable
     * - STRING can be decoded to [Char], as long as the string is a single character
     * - RECORD can be decoded to classes and objects, taking into account both [AvroAlias] and the [writerSchema]'s aliases to maximize compatibility
     * - ENUM can be decoded to [String] or [Enum] types, as long as the enum value is present in the schema
     *
     * @param deserializer the deserialization strategy to use for decoding the value. You may prefer the other extension methods without the deserializer parameter for convenience.
     * @param bytes the [ByteArray] to read the encoded value from.
     * @return the decoded value of type [T].
     *
     * @see AvroConfiguration
     * @see Avro.schema
     * @see AvroBuilder.serializersModule
     */
    override fun <T> decodeFromByteArray(
        deserializer: DeserializationStrategy<T>,
        bytes: ByteArray,
    ): T {
        return decodeFromByteArray(schema(deserializer.descriptor), deserializer, bytes)
    }

    /**
     * Encodes the given [value] to a [ByteArray] using the provided [serializer].
     * The schema is inferred from the [serializer]'s descriptor, and the value is encoded using that schema.
     *
     * You may prefer the other extension methods that infer the [serializer] based on a generic type [T].
     */
    override fun <T> encodeToByteArray(
        serializer: SerializationStrategy<T>,
        value: T,
    ): ByteArray {
        return encodeToByteArray(schema(serializer.descriptor), serializer, value)
    }
}

public fun Avro(
    from: Avro = Avro,
    builderAction: AvroBuilder.() -> Unit,
): Avro {
    val builder = AvroBuilder(from)
    builder.builderAction()
    return AvroImpl(builder.build(), from.serializersModule.overwriteWith(builder.serializersModule))
}

public class AvroBuilder internal constructor(avro: Avro) {
    /**
     * @see AvroConfiguration.fieldNamingStrategy
     */
    @ExperimentalAvro4kApi
    public var fieldNamingStrategy: FieldNamingStrategy = avro.configuration.fieldNamingStrategy

    /**
     * @see AvroConfiguration.implicitNulls
     */
    @ExperimentalAvro4kApi
    public var implicitNulls: Boolean = avro.configuration.implicitNulls

    /**
     * @see AvroConfiguration.implicitEmptyCollections
     */
    @ExperimentalAvro4kApi
    public var implicitEmptyCollections: Boolean = avro.configuration.implicitEmptyCollections

    /**
     * @see AvroConfiguration.validateSerialization
     */
    @ExperimentalAvro4kApi
    public var validateSerialization: Boolean = avro.configuration.validateSerialization

    /**
     * The serializers module to use for encoding, decoding, and inferring schemas from kotlin types.
     *
     * @see KSerializer
     * @see com.github.avrokotlin.avro4k.serializer.AvroSerializer
     */
    public var serializersModule: SerializersModule = EmptySerializersModule()

    private val logicalTypes: MutableMap<String, KSerializer<out Any>> = avro.configuration.logicalTypes.toMutableMap()

    /**
     * Registers a logical type serializer for the given [logicalTypeName].
     * This serializer will be used when decoding values of the given logical type.
     *
     * By default, [com.github.avrokotlin.avro4k.serializer.AnySerializer] decodes the following logical types:
     * - `duration` as [com.github.avrokotlin.avro4k.serializer.AvroDuration]
     * - `uuid` as [java.util.UUID]
     * - `date` as [java.time.LocalDate]
     * - `time-millis` as [java.time.LocalTime]
     * - `time-micros` as [java.time.LocalTime]
     * - `timestamp-millis` as [java.time.Instant]
     * - `timestamp-micros` as [java.time.Instant]
     *
     * @param logicalTypeName the name of the logical type to register the serializer for.
     * @param serializer the serializer to use for the logical type.
     *
     * @see AvroConfiguration.logicalTypes
     */
    @ExperimentalAvro4kApi
    public fun setLogicalTypeSerializer(
        logicalTypeName: String,
        serializer: KSerializer<out Any>,
    ) {
        logicalTypes[logicalTypeName] = serializer
    }

    internal fun build(): AvroConfiguration =
        AvroConfiguration(
            fieldNamingStrategy = fieldNamingStrategy,
            implicitNulls = implicitNulls,
            implicitEmptyCollections = implicitEmptyCollections,
            validateSerialization = validateSerialization,
            logicalTypes = logicalTypes
        )
}

private class AvroImpl(configuration: AvroConfiguration, serializersModule: SerializersModule) :
    Avro(configuration, serializersModule)

/**
 * Infers the Avro schema for the given type [T].
 *
 * @see Avro.schema
 */
public inline fun <reified T> Avro.schema(): Schema {
    val serializer = serializersModule.serializer<T>()
    return schema(serializer.descriptor)
}

/**
 * Infers the Avro schema for the given [serializer].
 *
 * @see Avro.schema
 */
public fun Avro.schema(serializer: KSerializer<*>): Schema {
    return schema(serializer.descriptor)
}

/**
 * Encodes the given [value] to a [ByteArray] using the provided [writerSchema].
 *
 * @see Avro.encodeToByteArray
 */
public inline fun <reified T> Avro.encodeToByteArray(
    writerSchema: Schema,
    value: T,
): ByteArray {
    val serializer = serializersModule.serializer<T>()
    return encodeToByteArray(writerSchema, serializer, value)
}

/**
 * Decode the value from the given [bytes] to a value of type [T] assuming the data is respecting the [writerSchema].
 * If during the decoding, the written data does not match [T]'s schema, you may end up to unexpected results or an exception.
 *
 * For any custom encoding or conversion of type [T] or its inner type, you may need to register a custom [kotlinx.serialization.KSerializer] in [Avro.serializersModule].
 *
 * @see Avro.decodeFromByteArray
 */
public inline fun <reified T> Avro.decodeFromByteArray(
    writerSchema: Schema,
    bytes: ByteArray,
): T {
    val serializer = serializersModule.serializer<T>()
    return decodeFromByteArray(writerSchema, serializer, bytes)
}