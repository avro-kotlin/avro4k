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

@ExperimentalSerializationApi
public inline fun <T : Any> AvroEncoder.encodeResolvingUnion(
    error: () -> Throwable,
    resolver: (Schema) -> T?,
): T {
    val schema = currentWriterSchema
    return if (schema.type == Schema.Type.UNION) {
        schema.types.firstNotNullOfOrNull(resolver)
    } else {
        resolver(schema)
    } ?: throw error()
}