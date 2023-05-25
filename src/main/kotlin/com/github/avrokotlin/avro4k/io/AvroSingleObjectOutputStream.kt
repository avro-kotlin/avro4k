package com.github.avrokotlin.avro4k.io

import kotlinx.serialization.ExperimentalSerializationApi
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.message.BinaryMessageEncoder
import java.io.OutputStream

/**
 * Uses [BinaryMessageEncoder] to write single object encoded bytes to given output stream.
 *
 * Instance <T> is converted to [GenericRecord] first.
 *
 * @param output target output stream, will be closed when this is closed
 * @param converter converts instance <T> to [GenericRecord] (default implementation in [com.github.avrokotlin.avro4k.Avro.openOutputStream].
 * @param writerSchema fingerprint included in single object bytes
 */
@OptIn(ExperimentalSerializationApi::class)
class AvroSingleObjectOutputStream<T>(
   private val output: OutputStream,
   private val converter: (T) -> GenericRecord,
   writerSchema: Schema
) : AvroOutputStream<T> {

   private val messageEncoder = BinaryMessageEncoder<GenericRecord>(GenericData.get(), writerSchema)

   override fun flush() {
   }

   override fun fSync() {}

   override fun close() {
      flush()
      output.close()
   }

   override fun write(t: T): AvroOutputStream<T> = also {
      messageEncoder.encode(converter(t), output)
   }
}
