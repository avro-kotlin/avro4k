package com.github.avrokotlin.avro4k.encoder.avro

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.AbstractEncoder
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import java.nio.ByteBuffer

@ExperimentalSerializationApi
class ByteArrayEncoder(private val schema: Schema,
                       override val serializersModule: SerializersModule,
                       private val callback: (Any) -> Unit) : AbstractEncoder() {

   private val bytes = mutableListOf<Byte>()

   override fun encodeByte(value: Byte) {
      bytes.add(value)
   }

   override fun endStructure(descriptor: SerialDescriptor) {
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
         //Wrapping the resulting byte array directly as this does not duplicate the byte array
         Schema.Type.BYTES -> callback(ByteBuffer.wrap(bytes.toByteArray()))
         else -> throw SerializationException("Cannot encode byte array when schema is ${schema.type}")
      }

   }
}