package com.sksamuel.avro4k.streams

import com.sksamuel.avro4k.Avro
import kotlinx.serialization.DeserializationStrategy
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory
import java.io.InputStream

/**
 * An implementation of [[AvroInputStream]] that reads values of type T
 * written as binary data.
 * See https://avro.apache.org/docs/current/spec.html#binary_encoding
 *
 * In order to convert the underlying binary data into types of T, this
 * input stream requires an instance of Decoder.
 */
class AvroBinaryInputStream<T>(private val source: InputStream,
                               private val deserializer: DeserializationStrategy<T>,
                               writerSchema: Schema,
                               private val readerSchema: Schema) : AvroInputStream<T> {

   private val datumReader = GenericDatumReader<GenericRecord>(writerSchema, readerSchema, GenericData())
   private val binaryDecoder = DecoderFactory.get().binaryDecoder(source, null)

   override fun next(): T? {
      val record = datumReader.read(null, binaryDecoder)
      return if (record == null) null else Avro.default.fromRecord(deserializer, readerSchema, record)
   }

   override fun close(): Unit = source.close()
}