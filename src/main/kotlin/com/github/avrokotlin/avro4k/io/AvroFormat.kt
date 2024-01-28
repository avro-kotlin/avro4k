@file:Suppress("DEPRECATION")

package com.github.avrokotlin.avro4k.io

@Deprecated("use AvroEncodeFormat and AvroDecodeFormat")
sealed class AvroFormat {
    /**
     * Binary format without the header, the most compact format.
     *
     * See https://avro.apache.org/docs/current/spec.html#binary_encoding
     */
    @Deprecated("use AvroEncodeFormat and AvroDecodeFormat")
    object BinaryFormat : AvroFormat()

    /**
     * Text format encoded as JSON. The most verbose format, but easy for a human to read.
     *
     * This format does not include the schema. Thus the write and read schema is needed for decoding.
     *
     * See https://avro.apache.org/docs/current/spec.html#json_encoding
     */
    @Deprecated("use AvroEncodeFormat and AvroDecodeFormat")
    object JsonFormat : AvroFormat()

    /**
     * Binary format with a header that contains the full schema, this is the format usually used when writing Avro files.
     *
     * See https://avro.apache.org/docs/current/spec.html#Data+Serialization+and+Deserialization
     */
    @Deprecated("use AvroEncodeFormat and AvroDecodeFormat")
    object DataFormat : AvroFormat()
}