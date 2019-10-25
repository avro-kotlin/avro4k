package com.sksamuel.avro4k.io

import com.sksamuel.avro4k.Avro
import kotlinx.serialization.DeserializationStrategy
import org.apache.avro.Schema
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DatumReader
import java.io.InputStream

@Suppress("UNCHECKED_CAST")
class AvroDataInputStream<T>(private val source: InputStream,
                             private val deserializer: DeserializationStrategy<T>,
                             writerSchema: Schema?,
                             private val readerSchema: Schema?) : AvroInputStream<T> {

   // if no reader or writer schema is specified, then we create a reader that uses what's present in the files
   private val datumReader = when {
      writerSchema == null && readerSchema == null -> GenericData.get().createDatumReader(null)
      readerSchema == null -> GenericData.get().createDatumReader(writerSchema)
      writerSchema == null -> GenericData.get().createDatumReader(readerSchema)
      else -> GenericData.get().createDatumReader(writerSchema, readerSchema)
   }

   private val dataFileReader = DataFileStream<GenericRecord>(source, datumReader as DatumReader<GenericRecord>)

   override fun next(): T? {
      return if (dataFileReader.hasNext()) {
         val record = dataFileReader.next(null)
         Avro.default.fromRecord(deserializer, record)
      } else null
   }

   override fun close(): Unit = source.close()
}
