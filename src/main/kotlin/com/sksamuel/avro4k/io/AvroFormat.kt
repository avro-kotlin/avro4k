package com.sksamuel.avro4k.io

sealed class AvroFormat {

   /**
    * Binary format without the header, the most compact format.
    *
    * See https://avro.apache.org/docs/current/spec.html#binary_encoding
    */
   object BinaryFormat : AvroFormat()

   /**
    * Text format encoded as JSON. The most verbose format, but easy for a human to read.
    *
    * See https://avro.apache.org/docs/current/spec.html#json_encoding
    */
   object JsonFormat : AvroFormat()

   /**
    * Binary format with an header that contains the full schema, this is the format usually used when writing Avro files.
    *
    * See https://avro.apache.org/docs/current/spec.html#Data+Serialization+and+Deserialization
    */
   object DataFormat : AvroFormat()
}