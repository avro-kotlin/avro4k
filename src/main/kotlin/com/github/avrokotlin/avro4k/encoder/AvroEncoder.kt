package com.github.avrokotlin.avro4k.encoder

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.encoding.Encoder
import org.apache.avro.Schema
import org.apache.avro.generic.GenericFixed
import java.nio.ByteBuffer

public interface AvroEncoder : Encoder {
    @ExperimentalSerializationApi
    public val currentWriterSchema: Schema

    @ExperimentalSerializationApi
    public fun encodeBytes(value: ByteBuffer)

    @ExperimentalSerializationApi
    public fun encodeBytes(value: ByteArray)

    @ExperimentalSerializationApi
    public fun encodeFixed(value: ByteArray)

    @ExperimentalSerializationApi
    public fun encodeFixed(value: GenericFixed)
}

@PublishedApi
internal interface UnionEncoder : AvroEncoder {
    fun selectUnionIndex(index: Int)

    override var currentWriterSchema: Schema
}

@ExperimentalSerializationApi
public inline fun <T : Any> AvroEncoder.encodeResolvingUnion(
    error: () -> Throwable,
    resolver: (Schema) -> (() -> T)?,
): T {
    val schema = currentWriterSchema
    return encodeResolvingUnion(schema, error, resolver)
}

@PublishedApi
internal inline fun <T : Any> AvroEncoder.encodeResolvingUnion(
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
internal inline fun <T> AvroEncoder.resolveUnion(
    schema: Schema,
    resolver: (Schema) -> (() -> T)?,
): T? {
    for (index in schema.types.indices) {
        val subSchema = schema.types[index]
        resolver(subSchema)?.let {
            (this as UnionEncoder).selectUnionIndex(index)
            return it.invoke()
        }
    }
    return null
}