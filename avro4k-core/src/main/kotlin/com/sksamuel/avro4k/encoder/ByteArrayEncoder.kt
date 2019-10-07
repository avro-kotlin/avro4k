package com.sksamuel.avro4k.encoder

import kotlinx.serialization.ElementValueEncoder
import kotlinx.serialization.SerialDescriptor
import kotlinx.serialization.SerializationException
import kotlinx.serialization.modules.SerialModule
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import java.nio.ByteBuffer

class ByteArrayEncoder(private val schema: Schema,
                       override val context: SerialModule,
                       private val callback: (Any) -> Unit) : ElementValueEncoder() {

   private val bytes = mutableListOf<Byte>()

   override fun encodeByte(value: Byte) {
      bytes.add(value)
   }

   override fun endStructure(desc: SerialDescriptor) {
      when (schema.type) {
         Schema.Type.FIXED -> {
            // the array passed in must be padded to size
            val padding = schema.fixedSize - bytes.size
            val padded = ByteBuffer.allocate(schema.fixedSize)
               .put(ByteArray(padding) { 0 })
               .put(bytes.toByteArray())
               .array()
            callback(GenericData.get().createFixed(null, padded, schema))
         }
         Schema.Type.BYTES -> callback(ByteBuffer.allocate(bytes.size).put(bytes.toByteArray()))
         else -> throw SerializationException("Cannot encode byte array when schema is ${schema.type}")
      }

   }
}