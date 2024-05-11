package com.github.avrokotlin.avro4k.decoder

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.encoding.Decoder
import org.apache.avro.Schema
import org.apache.avro.generic.GenericFixed

public interface AvroDecoder : Decoder {
    /**
     * Provides the schema used to encode the current value.
     * It won't return a union as the schema correspond to the actual value.
     */
    @ExperimentalSerializationApi
    public val currentWriterSchema: Schema

    @ExperimentalSerializationApi
    public fun decodeBytes(): ByteArray

    @ExperimentalSerializationApi
    public fun decodeFixed(): GenericFixed

    /**
     * Decode a value that corresponds to the [currentWriterSchema].
     *
     * You should prefer using directly [currentWriterSchema] to get the schema and then decode the value using the appropriate **decode*** method.
     */
    @ExperimentalSerializationApi
    public fun decodeValue(): Any
}

@ExperimentalSerializationApi
public inline fun <T : Any> AvroDecoder.decodeResolvingUnion(
    error: () -> Throwable,
    resolver: (Schema) -> (() -> T)?,
): T {
    val schema = currentWriterSchema
    return decodeResolvingUnion(schema, error, resolver)
}

@PublishedApi
internal inline fun <T : Any> AvroDecoder.decodeResolvingUnion(
    schema: Schema,
    error: () -> Throwable,
    resolver: (Schema) -> (() -> T)?,
): T {
    return if (schema.type == Schema.Type.UNION) {
        resolveUnion(schema, resolver)
    } else {
        resolver(schema)?.invoke()
    } ?: throw error()
}

@PublishedApi
internal inline fun <T> AvroDecoder.resolveUnion(
    schema: Schema,
    resolver: (Schema) -> (() -> T)?,
): T? {
    for (index in schema.types.indices) {
        val subSchema = schema.types[index]
        resolver(subSchema)?.let {
            return it.invoke()
        }
    }
    return null
}