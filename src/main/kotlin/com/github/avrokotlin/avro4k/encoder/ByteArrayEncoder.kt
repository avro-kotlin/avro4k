package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.schema.ensureOfType
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.AbstractEncoder
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import java.nio.ByteBuffer

@ExperimentalSerializationApi
class ZeroPaddedBytesEncoder(
   private val schema: Schema,
   private val actualSize: Int,
   override val serializersModule: SerializersModule,
   private val callback: (Any) -> Unit
) : AbstractEncoder() {
   private val bytes: ByteBuffer

   init {
      schema.ensureOfType(Schema.Type.FIXED)
      val padding = schema.fixedSize - actualSize
      check(padding >= 0) { "Fixed byte array overflow. Fixed size: ${schema.fixedSize}. Actual size: $actualSize" }
      bytes = ByteBuffer.allocate(schema.fixedSize)
         .apply {
            for (i in 0 until padding) {
               put(0)
            }
         }
   }

   override fun encodeByte(value: Byte) {
      bytes.put(value)
   }

   override fun endStructure(descriptor: SerialDescriptor) {
      callback(GenericData.get().createFixed(null, bytes.array(), schema))
   }
}

@ExperimentalSerializationApi
class BytesEncoder(
   actualSize: Int,
   override val serializersModule: SerializersModule,
   private val callback: (Any) -> Unit
) : AbstractEncoder() {
   private val bytes: ByteBuffer = ByteBuffer.allocate(actualSize)

   override fun encodeByte(value: Byte) {
      bytes.put(value)
   }

   override fun endStructure(descriptor: SerialDescriptor) {
      callback(bytes.position(0))
   }
}
