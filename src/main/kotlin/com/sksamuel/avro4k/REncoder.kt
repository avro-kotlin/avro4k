package com.sksamuel.avro4k

import kotlinx.serialization.NamedValueEncoder
import org.apache.avro.AvroRuntimeException
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import java.nio.ByteBuffer

class REncoder(private val schema: Schema,
               val builder: RecordBuilder) : NamedValueEncoder() {

   override fun encodeTaggedString(tag: String, value: String) {
      val fixed = schema.getField(tag).schema()
      val v = when (fixed.type) {
         Schema.Type.FIXED -> {
            val size = value.toByteArray().size
            if (size > fixed.fixedSize)
               throw AvroRuntimeException("Cannot write string with $size bytes to fixed type of size ${fixed.fixedSize}")
            // the array passed in must be padded to size
            val bytes = ByteBuffer.allocate(fixed.fixedSize).put(value.toByteArray()).array()
            GenericData.get().createFixed(null, bytes, fixed)
         }
         Schema.Type.BYTES -> ByteBuffer.wrap(value.toByteArray())
         else -> Utf8(value)
      }
      builder.add(tag, v)
   }

   override fun encodeTaggedDouble(tag: String, value: Double) {
      builder.add(tag, value)
   }

   override fun encodeTaggedLong(tag: String, value: Long) {
      builder.add(tag, value)
   }

   override fun encodeTaggedByte(tag: String, value: Byte) {
      builder.add(tag, value)
   }

   override fun encodeTaggedBoolean(tag: String, value: Boolean) {
      builder.add(tag, value)
   }

   override fun encodeTaggedShort(tag: String, value: Short) {
      builder.add(tag, value)
   }

   override fun encodeTaggedInt(tag: String, value: Int) {
      builder.add(tag, value)
   }

   override fun encodeTaggedFloat(tag: String, value: Float) {
      builder.add(tag, value)
   }
}

class RecordBuilder(private val schema: Schema) {

   private val map = mutableMapOf<String, Any?>()

   fun add(key: String, value: Any?) {
      map[key] = value
   }

   fun record(): Record {
      return MapRecord(schema, map)
   }
}