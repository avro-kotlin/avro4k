package com.sksamuel.avro4k

import kotlinx.serialization.CompositeEncoder
import kotlinx.serialization.ElementValueEncoder
import kotlinx.serialization.KSerializer
import kotlinx.serialization.PrimitiveKind
import kotlinx.serialization.SerialDescriptor
import kotlinx.serialization.SerializationException
import kotlinx.serialization.StructureKind
import kotlinx.serialization.internal.EnumDescriptor
import org.apache.avro.AvroRuntimeException
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import java.nio.ByteBuffer

class RecordEncoder(private val schema: Schema,
                    val callback: (Record) -> Unit) : ElementValueEncoder() {

   private val builder = ArrayRecordBuilder(schema)
   private var currentIndex = -1

   private fun fieldSchema() = schema.fields[currentIndex].schema()

   override fun encodeString(value: String) {
      builder.add(StringToValue.toValue(fieldSchema(), value))
   }

   override fun encodeValue(value: Any) {
      builder.add(value)
   }

   override fun encodeElement(desc: SerialDescriptor, index: Int): Boolean {
      currentIndex = index
      return super.encodeElement(desc, index)
   }

   override fun encodeEnum(enumDescription: EnumDescriptor, ordinal: Int) {
      val field = schema.fields[currentIndex]
      val symbol = enumDescription.getElementName(ordinal)
      val generic = GenericData.get().createEnum(symbol, field.schema())
      builder.add(generic)
   }

   override fun beginStructure(desc: SerialDescriptor, vararg typeParams: KSerializer<*>): CompositeEncoder {
      if (currentIndex == -1) return this
      return when (desc.kind) {
         StructureKind.LIST -> {
            when (desc.getElementDescriptor(0).kind) {
               PrimitiveKind.BYTE -> ByteArrayEncoder(fieldSchema()) { builder.add(it) }
               else -> ListEncoder(fieldSchema()) { builder.add(it) }
            }
         }
         StructureKind.CLASS -> RecordEncoder(fieldSchema()) { builder.add(it) }
         else -> this
      }
   }

   override fun endStructure(desc: SerialDescriptor) {
      callback(builder.record())
   }

   override fun encodeNull() {
      builder.add(null)
   }
}

class ByteArrayEncoder(private val schema: Schema,
                       private val callback: (Any) -> Unit) : ElementValueEncoder() {

   private val bytes = mutableListOf<Byte>()

   override fun encodeByte(value: Byte) {
      bytes.add(value)
   }

   override fun endStructure(desc: SerialDescriptor) {
      when (schema.type) {
         Schema.Type.FIXED -> callback(GenericData.get().createFixed(null, bytes.toByteArray(), schema))
         Schema.Type.BYTES -> callback(ByteBuffer.allocate(bytes.size).put(bytes.toByteArray()))
         else -> throw SerializationException("Cannot encode byte array when schema is ${schema.type}")
      }

   }
}

class ListEncoder(private val schema: Schema,
                  private val callback: (GenericData.Array<Any?>) -> Unit) : ElementValueEncoder() {

   private val list = mutableListOf<Any?>()

   override fun endStructure(desc: SerialDescriptor) {
      val generic = GenericData.Array(schema, list.toList())
      callback(generic)
   }

   override fun encodeString(value: String) {
      list.add(StringToValue.toValue(schema, value))
   }

   override fun encodeLong(value: Long) {
      list.add(value)
   }

   override fun encodeDouble(value: Double) {
      list.add(value)
   }

   override fun encodeBoolean(value: Boolean) {
      list.add(value)
   }

   override fun encodeShort(value: Short) {
      list.add(value)
   }

   override fun encodeByte(value: Byte) {
      list.add(value)
   }

   override fun encodeFloat(value: Float) {
      list.add(value)
   }

   override fun encodeInt(value: Int) {
      list.add(value)
   }
}

class MapRecordBuilder(private val schema: Schema) {

   private val map = mutableMapOf<String, Any?>()

   fun add(key: String, value: Any?) {
      map[key] = value
   }

   fun record(): Record {
      return MapRecord(schema, map)
   }
}

class ArrayRecordBuilder(private val schema: Schema) {

   private val values = arrayListOf<Any?>()

   fun add(value: Any?) = values.add(value)

   fun record(): Record = ListRecord(schema, values)
}

interface ToValue<T> {
   fun toValue(schema: Schema, t: T): Any
}

object StringToValue : ToValue<String> {
   override fun toValue(schema: Schema, t: String): Any {
      return when (schema.type) {
         Schema.Type.FIXED -> {
            val size = t.toByteArray().size
            if (size > schema.fixedSize)
               throw AvroRuntimeException("Cannot write string with $size bytes to fixed type of size ${schema.fixedSize}")
            // the array passed in must be padded to size
            val bytes = ByteBuffer.allocate(schema.fixedSize).put(t.toByteArray()).array()
            GenericData.get().createFixed(null, bytes, schema)
         }
         Schema.Type.BYTES -> ByteBuffer.wrap(t.toByteArray())
         else -> Utf8(t)
      }
   }
}

