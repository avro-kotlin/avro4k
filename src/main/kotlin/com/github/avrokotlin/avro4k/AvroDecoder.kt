package com.github.avrokotlin.avro4k

import com.github.avrokotlin.avro4k.internal.decoder.direct.AbstractAvroDirectDecoder
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.encoding.Decoder
import org.apache.avro.Schema
import org.apache.avro.generic.GenericFixed

/**
 * Interface to decode Avro values.
 * Here are the main methods to decode values. Each decode method is adapting the raw type to the wanted type, that means unions are resolved if needed, and also all primitives are converted automatically (a wanted `long` could be decoded from a `int`).
 *
 * Primitives:
 * - [decodeNull]
 * - [decodeBoolean]
 * - [decodeByte]
 * - [decodeShort]
 * - [decodeInt]
 * - [decodeLong]
 * - [decodeFloat]
 * - [decodeDouble]
 * - [decodeString]
 * - [decodeChar]
 * - [decodeEnum]
 *
 * Avro specific:
 * - [decodeBytes]
 * - [decodeFixed]
 *
 * Use the following methods to allow complex decoding using raw values, mainly for logical types:
 * - [decodeResolvingAny]
 * - [decodeResolvingBoolean]
 * - [decodeResolvingByte]
 * - [decodeResolvingShort]
 * - [decodeResolvingInt]
 * - [decodeResolvingLong]
 * - [decodeResolvingFloat]
 * - [decodeResolvingDouble]
 * - [decodeResolvingChar]
 */
public interface AvroDecoder : Decoder {
    /**
     * Provides the schema used to encode the current value.
     *
     * It won't return a union as the schema correspond to the actual value.
     */
    @ExperimentalSerializationApi
    public val currentWriterSchema: Schema

    /**
     * Decode a [Schema.Type.BYTES] value.
     *
     * A bytes value is a sequence of bytes prefixed with an int corresponding to its length.
     */
    @ExperimentalSerializationApi
    public fun decodeBytes(): ByteArray

    /**
     * Decode a [Schema.Type.FIXED] value.
     *
     * A fixed value is a fixed-size sequence of bytes, where the length is not materialized in the binary output as it is known by the [currentWriterSchema].
     */
    @ExperimentalSerializationApi
    public fun decodeFixed(): GenericFixed

    /**
     * Decode a value that corresponds to the [currentWriterSchema].
     *
     * You should prefer using directly [currentWriterSchema] to get the schema and then decode the value using the appropriate **decode*** method.
     *
     * Will be removed in the future as direct decoding isn't capable of it.
     */
    @Deprecated("Use currentWriterSchema to get the schema and then decode the value using the appropriate decode* method, or use decodeResolving* for more complex use cases.")
    @ExperimentalSerializationApi
    public fun decodeValue(): Any
}

/**
 * Allows you to decode a value differently depending on the schema (generally its name, type, logicalType), even if it is a union.
 *
 * This reduces the need to manually resolve the type in a union **and** not in a union.
 *
 * For examples, see the [com.github.avrokotlin.avro4k.serializer.BigDecimalSerializer] as it resolves a lot of types and also logical types.
 *
 * **Important note:** Use the specific methods for primitives to avoid auto-boxing and improve performances.
 *
 * @param resolver A lambda that returns a [AnyValueDecoder] that contains the logic to decode the value only when the schema matches. The decoding **MUST** be done in the [AnyValueDecoder] to avoid decoding the value if it is not the right schema. Return null when it is not matching the expected schema.
 * @param error A lambda that throws an exception if the decoder cannot be resolved.
 *
 * @see decodeResolvingBoolean
 * @see decodeResolvingByte
 * @see decodeResolvingShort
 * @see decodeResolvingInt
 * @see decodeResolvingLong
 * @see decodeResolvingFloat
 * @see decodeResolvingDouble
 * @see decodeResolvingChar
 */
@ExperimentalSerializationApi
public inline fun <T : Any> AvroDecoder.decodeResolvingAny(
    error: () -> Throwable,
    resolver: (Schema) -> AnyValueDecoder<T>?,
): T {
    return findValueDecoder(error, resolver).decodeAny()
}

/**
 * An [Byte] specific [decodeResolvingAny] to prevent auto-boxing (improving performances avoiding primitive<->object conversions).
 *
 * @see decodeResolvingAny
 */
@ExperimentalSerializationApi
public inline fun AvroDecoder.decodeResolvingByte(
    error: () -> Throwable,
    resolver: (Schema) -> ByteValueDecoder?,
): Byte {
    return findValueDecoder(error, resolver).decodeByte()
}

/**
 * An [Short] specific [decodeResolvingAny] to prevent auto-boxing (improving performances avoiding primitive<->object conversions).
 *
 * @see decodeResolvingAny
 */
@ExperimentalSerializationApi
public inline fun AvroDecoder.decodeResolvingShort(
    error: () -> Throwable,
    resolver: (Schema) -> ShortValueDecoder?,
): Short {
    return findValueDecoder(error, resolver).decodeShort()
}

/**
 * An [Int] specific [decodeResolvingAny] to prevent auto-boxing (improving performances avoiding primitive<->object conversions).
 *
 * @see decodeResolvingAny
 */
@ExperimentalSerializationApi
public inline fun AvroDecoder.decodeResolvingInt(
    error: () -> Throwable,
    resolver: (Schema) -> IntValueDecoder?,
): Int {
    return findValueDecoder(error, resolver).decodeInt()
}

/**
 * A [Long] specific [decodeResolvingAny] to prevent auto-boxing (improving performances avoiding primitive<->object conversions).
 *
 * @see decodeResolvingAny
 */
@ExperimentalSerializationApi
public inline fun AvroDecoder.decodeResolvingLong(
    error: () -> Throwable,
    resolver: (Schema) -> LongValueDecoder?,
): Long {
    return findValueDecoder(error, resolver).decodeLong()
}

/**
 * A [Boolean] specific [decodeResolvingAny] to prevent auto-boxing (improving performances avoiding primitive<->object conversions).
 *
 * @see decodeResolvingAny
 */
@ExperimentalSerializationApi
public inline fun AvroDecoder.decodeResolvingBoolean(
    error: () -> Throwable,
    resolver: (Schema) -> BooleanValueDecoder?,
): Boolean {
    return findValueDecoder(error, resolver).decodeBoolean()
}

/**
 * A [Float] specific [decodeResolvingAny] to prevent auto-boxing (improving performances avoiding primitive<->object conversions).
 *
 * @see decodeResolvingAny
 */
@ExperimentalSerializationApi
public inline fun AvroDecoder.decodeResolvingFloat(
    error: () -> Throwable,
    resolver: (Schema) -> FloatValueDecoder?,
): Float {
    return findValueDecoder(error, resolver).decodeFloat()
}

/**
 * A [Double] specific [decodeResolvingAny] to prevent auto-boxing (improving performances avoiding primitive<->object conversions).
 *
 * @see decodeResolvingAny
 */
@ExperimentalSerializationApi
public inline fun AvroDecoder.decodeResolvingDouble(
    error: () -> Throwable,
    resolver: (Schema) -> DoubleValueDecoder?,
): Double {
    return findValueDecoder(error, resolver).decodeDouble()
}

/**
 * A [Char] specific [decodeResolvingAny] to prevent auto-boxing (improving performances avoiding primitive<->object conversions).
 *
 * @see decodeResolvingAny
 */
@ExperimentalSerializationApi
public inline fun AvroDecoder.decodeResolvingChar(
    error: () -> Throwable,
    resolver: (Schema) -> CharValueDecoder?,
): Char {
    return findValueDecoder(error, resolver).decodeChar()
}

/**
 * @see AvroDecoder.decodeResolvingAny
 */
@ExperimentalSerializationApi
public fun interface AnyValueDecoder<T> {
    context(AvroDecoder)
    public fun decodeAny(): T
}

/**
 * @see AvroDecoder.decodeResolvingBoolean
 */
@ExperimentalSerializationApi
public fun interface BooleanValueDecoder {
    context(AvroDecoder)
    public fun decodeBoolean(): Boolean
}

/**
 * @see AvroDecoder.decodeResolvingByte
 */
@ExperimentalSerializationApi
public fun interface ByteValueDecoder {
    context(AvroDecoder)
    public fun decodeByte(): Byte
}

/**
 * @see AvroDecoder.decodeResolvingShort
 */
@ExperimentalSerializationApi
public fun interface ShortValueDecoder {
    context(AvroDecoder)
    public fun decodeShort(): Short
}

/**
 * @see AvroDecoder.decodeResolvingInt
 */
@ExperimentalSerializationApi
public fun interface IntValueDecoder {
    context(AvroDecoder)
    public fun decodeInt(): Int
}

/**
 * @see AvroDecoder.decodeResolvingLong
 */
@ExperimentalSerializationApi
public fun interface LongValueDecoder {
    context(AvroDecoder)
    public fun decodeLong(): Long
}

/**
 * @see AvroDecoder.decodeResolvingFloat
 */
@ExperimentalSerializationApi
public fun interface FloatValueDecoder {
    context(AvroDecoder)
    public fun decodeFloat(): Float
}

/**
 * @see AvroDecoder.decodeResolvingDouble
 */
@ExperimentalSerializationApi
public fun interface DoubleValueDecoder {
    context(AvroDecoder)
    public fun decodeDouble(): Double
}

/**
 * @see AvroDecoder.decodeResolvingChar
 */
@ExperimentalSerializationApi
public fun interface CharValueDecoder {
    context(AvroDecoder)
    public fun decodeChar(): Char
}

@PublishedApi
internal inline fun <T : Any> AvroDecoder.findValueDecoder(
    error: () -> Throwable,
    resolver: (Schema) -> T?,
): T {
    val schema = currentWriterSchema

    val foundResolver =
        if (schema.isUnion) {
            if (this is AbstractAvroDirectDecoder) {
                throw UnsupportedOperationException("The union should be already resolved, which means a misusage of avro4k")
            } else {
                currentWriterSchema.types.firstNotNullOfOrNull(resolver)
            }
        } else {
            resolver(schema)
        }
    return foundResolver ?: throw error()
}

internal fun AvroDecoder.unsupportedWriterTypeError(
    mainType: Schema.Type,
    vararg fallbackTypes: Schema.Type,
): Throwable {
    val fallbacksStr = if (fallbackTypes.isNotEmpty()) ", and also not matching to any compatible type (one of ${fallbackTypes.joinToString()})." else ""
    return SerializationException(
        "Unsupported schema named '${currentWriterSchema.fullName}' for decoded type of $mainType$fallbacksStr. Actual schema: $currentWriterSchema"
    )
}