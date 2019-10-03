package com.sksamuel.avro4k.streams

import com.sksamuel.avro4k.Avro
import kotlinx.serialization.SerializationStrategy
import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.BinaryEncoder
import org.apache.avro.io.Encoder
import org.apache.avro.io.EncoderFactory
import org.apache.avro.io.JsonEncoder
import java.io.OutputStream

abstract class DefaultAvroOutputStream<T>(private val output: OutputStream,
                                          private val serializer: SerializationStrategy<T>,
                                          schema: Schema) : AvroOutputStream<T> {

   val datumWriter = GenericDatumWriter<GenericRecord>(schema)

   abstract val encoder: Encoder

   override fun close() {
      flush()
      output.close()
   }

   override fun flush(): Unit = encoder.flush()
   override fun fSync() {}

   override fun write(t: T) {
      val record = Avro.default.toRecord(serializer, t)
      datumWriter.write(record, encoder)
   }
}

class AvroBinaryOutputStream<T>(output: OutputStream,
                                serializer: SerializationStrategy<T>,
                                schema: Schema) : DefaultAvroOutputStream<T>(output, serializer, schema) {

   override val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(output, null)
}

class AvroJsonOutputStream<T>(output: OutputStream,
                              serializer: SerializationStrategy<T>,
                              schema: Schema) : DefaultAvroOutputStream<T>(output, serializer, schema) {
   override val encoder: JsonEncoder = EncoderFactory.get().jsonEncoder(schema, output)
}
