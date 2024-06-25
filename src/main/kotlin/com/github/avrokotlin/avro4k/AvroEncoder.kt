package com.github.avrokotlin.avro4k

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.encoding.Encoder
import org.apache.avro.Schema
import org.apache.avro.generic.GenericFixed
import java.nio.ByteBuffer

/**
 * Interface to encode Avro values.
 * Here are the main methods to encode values. Each encode method is adapting the type to the raw type, that means unions are resolved if needed, and also all primitives are converted automatically (a wanted `int` could be encoded to a `long`).
 * - [encodeNull]
 * - [encodeBoolean]
 * - [encodeByte]
 * - [encodeShort]
 * - [encodeInt]
 * - [encodeLong]
 * - [encodeFloat]
 * - [encodeDouble]
 * - [encodeString]
 * - [encodeChar]
 * - [encodeEnum]
 * - [encodeBytes]
 * - [encodeFixed]
 *
 * Use the following methods to allow complex encoding using raw values, mainly for logical types:
 * - [encodeResolving]
 */
public interface AvroEncoder : Encoder {
    /**
     * Provides the schema used to encode the current value.
     */
    @ExperimentalSerializationApi
    public val currentWriterSchema: Schema

    /**
     * Encodes a [Schema.Type.BYTES] value from a [ByteBuffer].
     */
    @ExperimentalSerializationApi
    public fun encodeBytes(value: ByteBuffer)

    /**
     * Encodes a [Schema.Type.BYTES] value from a [ByteArray].
     */
    @ExperimentalSerializationApi
    public fun encodeBytes(value: ByteArray)

    /**
     * Encodes a [Schema.Type.FIXED] value from a [ByteArray]. Its size must match the size of the fixed schema in [currentWriterSchema].
     */
    @ExperimentalSerializationApi
    public fun encodeFixed(value: ByteArray)

    /**
     * Encodes a [Schema.Type.FIXED] value from a [GenericFixed]. Its size must match the size of the fixed schema in [currentWriterSchema].
     */
    @ExperimentalSerializationApi
    public fun encodeFixed(value: GenericFixed)
}

@PublishedApi
internal interface UnionEncoder : AvroEncoder {
    /**
     * Encode the selected union schema and set the selected type in [currentWriterSchema].
     */
    fun encodeUnionIndex(index: Int)
}

/**
 * Allows you to encode a value differently depending on the schema (generally its name, type, logicalType).
 * If the [AvroEncoder.currentWriterSchema] is a union, it takes **the first matching encoder** as the final encoder.
 *
 * This reduces the need to manually resolve the type in a union **and** not in a union.
 *
 * For examples, see the [com.github.avrokotlin.avro4k.serializer.BigDecimalSerializer] as it resolves a lot of types and also logical types.
 *
 * @param resolver A lambda that returns a lambda (the encoding lambda) that contains the logic to encode the value only when the schema matches. The encoding **MUST** be done in the encoder lambda to avoid encoding the value if it is not the right schema. Return null when it is not matching the expected schema.
 * @param error A lambda that throws an exception if the encoder cannot be resolved.
 */
@ExperimentalSerializationApi
public inline fun <T : Any> AvroEncoder.encodeResolving(
    error: () -> Throwable,
    resolver: (Schema) -> (() -> T)?,
): T {
    val schema = currentWriterSchema
    return if (schema.type == Schema.Type.UNION) {
        resolveUnion(schema, error, resolver)
    } else {
        resolver(schema)?.invoke() ?: throw error()
    }
}

@PublishedApi
internal inline fun <T> AvroEncoder.resolveUnion(
    schema: Schema,
    error: () -> Throwable,
    resolver: (Schema) -> (() -> T)?,
): T {
    for (index in schema.types.indices) {
        val subSchema = schema.types[index]
        resolver(subSchema)?.let {
            (this as UnionEncoder).encodeUnionIndex(index)
            return it.invoke()
        }
    }
    throw error()
}