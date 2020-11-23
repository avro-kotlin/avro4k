package com.github.avrokotlin.avro4k.io

import org.apache.avro.Schema
import org.apache.avro.file.CodecFactory
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import java.io.OutputStream

/**
 * An [AvroOutputStream] that writes the schema along with the messages.
 *
 * This is usually the format required when writing multiple messages to a single file.
 *
 * Some frameworks, such as a Kafka, store the Schema separately to messages, in which
 * case the [AvroBinaryInputStream], which does not include the schema, might be more appropriate.
 *
 * @param output  the underlying stream that data will be written to.
 * @param converter used to convert the input type into a [GenericRecord]
 * @param schema  the schema that will be used to encode the data, sometimes called the writer schema
 * @param codec   compression codec
 */
class AvroDataOutputStream<T>(private val output: OutputStream,
                              private val converter: (T) -> GenericRecord,
                              private val schema: Schema,
                              private val codec: CodecFactory) : AvroOutputStream<T> {

   private val datumWriter = GenericDatumWriter<GenericRecord>(schema)
   
   private val writer = DataFileWriter(datumWriter).apply {
      setCodec(codec)
      create(schema, output)
   }

   override fun close() {
      flush()
      writer.close()
   }

   override fun flush(): Unit = writer.flush()
   override fun fSync(): Unit = writer.fSync()

   override fun write(t: T): AvroOutputStream<T> {
      writer.append(converter(t))
      return this
   }
}