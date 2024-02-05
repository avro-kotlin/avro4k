package com.github.avrokotlin.avro4k.io

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.message.BinaryMessageDecoder
import org.apache.avro.message.SchemaStore
import java.io.InputStream

/**
 * Uses [BinaryMessageDecoder] to read single object encoded byteStream to [GenericRecord]
 * and then converts it to <T> using [com.github.avrokotlin.avro4k.Avro.fromRecord].
 *
 * @param input inputStream created from single object encoded bytes
 * @param converter function to convert from GenericRecord to instance of <T>
 * @param schemaStore avro [SchemaStore] to look up the writer schema encoded in the single object bytes
 * @param readerSchema the schema of the target class, needed for creation of instance <T> and compatibility checks between reader and writer schema
 */
class AvroSingleObjectInputStream<T>(
   private val input: InputStream,
   private val converter: (Any) -> T,
   schemaStore: SchemaStore,
   readerSchema: Schema
) : AvroInputStream<T> {

   private val messageDecoder = BinaryMessageDecoder<GenericRecord>(GenericData.get(), readerSchema, schemaStore)

   override fun next(): T? = messageDecoder.decode(input)?.let(converter)

   override fun close() = input.close()
}
