package com.sksamuel.avro4k.io

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
                                          private val converter: (T) -> GenericRecord,
                                          schema: Schema) : AvroOutputStream<T> {

   private val datumWriter = GenericDatumWriter<GenericRecord>(schema)

   abstract val encoder: Encoder

   override fun close() {
      flush()
      output.close()
   }

   override fun flush(): Unit = encoder.flush()
   override fun fSync() {}

   override fun write(t: T): AvroOutputStream<T> {
      datumWriter.write(converter(t), encoder)
      return this
   }
}

class AvroBinaryOutputStream<T>(output: OutputStream,
                                converter: (T) -> GenericRecord,
                                schema: Schema) : DefaultAvroOutputStream<T>(output, converter, schema) {
   constructor(output: OutputStream,
               serializer: SerializationStrategy<T>,
               schema: Schema,
               avro: Avro = Avro.default) : this(output, { avro.toRecord(serializer, it) }, schema)

   override val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(output, null)
}

class AvroJsonOutputStream<T>(output: OutputStream,
                              converter: (T) -> GenericRecord,
                              schema: Schema) : DefaultAvroOutputStream<T>(output, converter, schema) {
   constructor(output: OutputStream,
               serializer: SerializationStrategy<T>,
               schema: Schema,
               avro: Avro = Avro.default) : this(output, { avro.toRecord(serializer, it) }, schema)

   override val encoder: JsonEncoder = EncoderFactory.get().jsonEncoder(schema, output, true)
}
