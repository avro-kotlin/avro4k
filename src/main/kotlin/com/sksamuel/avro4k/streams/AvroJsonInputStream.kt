package com.sksamuel.avro4k.streams

import com.sksamuel.avro4k.Avro
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.SerializationException
import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory
import java.io.InputStream

class AvroJsonInputStream<T : Any>(private val source: InputStream,
                                   private val deserializer: DeserializationStrategy<T>,
                                   writerSchema: Schema,
                                   private val readerSchema: Schema) : AvroInputStream<T> {

   private val datumReader = GenericDatumReader<GenericRecord>(writerSchema, readerSchema)
   private val jsonDecoder = DecoderFactory.get().jsonDecoder(writerSchema, source)

   override fun next(): T? {
      val record = datumReader.read(null, jsonDecoder)
      return if (record == null) null else Avro.default.fromRecord(deserializer, readerSchema, record)
   }

   override fun seq(): Sequence<T> = generateSequence { next() }

   fun singleEntity(): T {
      return next() ?: throw SerializationException("No single entity contained in stream")
   }

   override fun close(): Unit = source.close()
}
