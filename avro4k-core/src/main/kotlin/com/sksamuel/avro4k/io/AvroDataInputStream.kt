package com.sksamuel.avro4k.io

import org.apache.avro.Schema
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.GenericData
import java.io.InputStream

@Suppress("UNCHECKED_CAST")
class AvroDataInputStream<T>(private val source: InputStream,
                             private val converter: (Any) -> T,
                             writerSchema: Schema?,
                             readerSchema: Schema?) : AvroInputStream<T> {

   // if no reader or writer schema is specified, then we create a reader that uses what's present in the files
   private val datumReader = when {
      writerSchema == null && readerSchema == null -> GenericData.get().createDatumReader(null)
      readerSchema == null -> GenericData.get().createDatumReader(writerSchema)
      writerSchema == null -> GenericData.get().createDatumReader(readerSchema)
      else -> GenericData.get().createDatumReader(writerSchema, readerSchema)
   }

   private val dataFileReader = DataFileStream<Any>(source, datumReader)

   override fun next(): T? {
      return if (dataFileReader.hasNext()) {
         val obj = dataFileReader.next(null)
         converter(obj)
      } else null
   }

   override fun close(): Unit = source.close()
}
