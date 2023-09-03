package com.github.avrokotlin.avro4k.io

import org.apache.avro.Schema
import org.apache.avro.file.CodecFactory
import java.io.OutputStream

/**
 * Formats that can be used to encode a record to an [OutputStream].
 */
sealed class AvroEncodeFormat {
    abstract fun <T> createOutputStream(output : OutputStream, schema : Schema, converter : (T) -> Any?) : AvroOutputStream<T>
    /**
     * Encodes a record in a binary format with a header that contains the full schema, this is the format usually used when writing Avro files.
     *
     * See https://avro.apache.org/docs/current/spec.html#Data+Serialization+and+Deserialization
     */
    data class Data(private val codecFactory: CodecFactory = CodecFactory.nullCodec()) : AvroEncodeFormat() {
        override fun <T> createOutputStream(
            output: OutputStream,
            schema: Schema,
            converter: (T) -> Any?
        ): AvroOutputStream<T> {
            return AvroDataOutputStream(output, converter, schema, codecFactory)
        }
    }
    /**
     * Encodes the record in a binary format without schema information, the most compact format.
     *
     * See https://avro.apache.org/docs/current/spec.html#binary_encoding
     */
    object Binary : AvroEncodeFormat() {
        override fun <T> createOutputStream(
            output: OutputStream,
            schema: Schema,
            converter: (T) -> Any?
        ) = AvroBinaryOutputStream(output, converter, schema)

    }
    /**
     * Encodes the avro records as JSON text. The most verbose format, but easy for a human to read.
     *
     * This format does not include the schema.
     *
     * See https://avro.apache.org/docs/current/spec.html#json_encoding
     */
    object Json : AvroEncodeFormat() {
        override fun <T> createOutputStream(
            output: OutputStream,
            schema: Schema,
            converter: (T) -> Any?
        ) = AvroJsonOutputStream(output, converter, schema)
    }
}