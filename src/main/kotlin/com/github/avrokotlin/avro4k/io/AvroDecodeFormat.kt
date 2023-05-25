package com.github.avrokotlin.avro4k.io

import org.apache.avro.Schema
import org.apache.avro.message.SchemaStore
import java.io.InputStream

/**
 * Formats that can be used to decode the contents of an [InputStream].
 */
sealed class AvroDecodeFormat {
    abstract fun <T> createInputStream(source : InputStream, converter: (Any) -> T) : AvroInputStream<T>
    /**
     * Decoding a binary format with a header that contains the full schema, this is the format usually used for Avro files.
     *
     * See https://avro.apache.org/docs/current/spec.html#Data+Serialization+and+Deserialization
     */
    data class Data(val writerSchema : Schema?, val readerSchema : Schema?) : AvroDecodeFormat() {
        constructor(readWriteSchema : Schema?) : this(readWriteSchema, readWriteSchema)
        override fun <T> createInputStream(source : InputStream, converter: (Any) -> T) = when{
                writerSchema != null && readerSchema != null ->
                    AvroDataInputStream(source, converter, writerSchema, readerSchema)
                writerSchema != null ->
                    AvroDataInputStream(source, converter, writerSchema, null)
                readerSchema != null ->
                    AvroDataInputStream(source, converter, null, readerSchema)
                else ->
                    AvroDataInputStream(source, converter, null, null)
        }
    }
    /**
     * Decodes the binary format without the header, the most compact format.
     *
     * See https://avro.apache.org/docs/current/spec.html#binary_encoding
     */
    data class Binary(val writerSchema: Schema, val readerSchema: Schema) : AvroDecodeFormat() {
        constructor(readWriteSchema : Schema) : this(readWriteSchema, readWriteSchema)
        override fun <T> createInputStream(source : InputStream, converter: (Any) -> T) =  AvroBinaryInputStream(source, converter, writerSchema, readerSchema)
    }
    /**
     * Decodes avro records that have been encoded in JSON. The most verbose format, but easy for a human to read.
     *
     * The avro json format does not include the schema. Thus the write and read schema is needed for decoding.
     *
     * See https://avro.apache.org/docs/current/spec.html#json_encoding
     */
    data class Json(val writerSchema: Schema, val readerSchema: Schema) : AvroDecodeFormat() {
        constructor(readWriteSchema : Schema) : this(readWriteSchema, readWriteSchema)
        override fun <T> createInputStream(source : InputStream, converter: (Any) -> T) =  AvroJsonInputStream(source, converter, writerSchema, readerSchema)
    }

   /**
    * Decodes avro records that have been encoded in single object format. This format is best suited for long time storage
    * as it is small and contains a fingerprint reference to the writer schema without including the schema content.
    *
    * See See https://avro.apache.org/docs/current/specification/#single-object-encoding
    *
    * @param readerSchema the schema we wil use to read the bytes payload
    * @param schemaStore will be used to look up the writer schema to write the payload to GenericRecord.
    */
   data class SingleObject(val readerSchema: Schema, val schemaStore: SchemaStore) : AvroDecodeFormat() {

      override fun <T> createInputStream(source: InputStream, converter: (Any) -> T) = AvroSingleObjectInputStream(
         readerSchema = readerSchema,
         input = source,
         converter = converter,
         schemaStore = schemaStore
      )
   }
}