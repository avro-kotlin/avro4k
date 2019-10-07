package com.sksamuel.avro4k.encoder

import com.sksamuel.avro4k.ListRecord
import com.sksamuel.avro4k.Record
import kotlinx.serialization.CompositeEncoder
import kotlinx.serialization.ElementValueEncoder
import kotlinx.serialization.KSerializer
import kotlinx.serialization.PrimitiveKind
import kotlinx.serialization.SerialDescriptor
import kotlinx.serialization.SerializationException
import kotlinx.serialization.StructureKind
import kotlinx.serialization.internal.EnumDescriptor
import kotlinx.serialization.modules.SerialModule
import org.apache.avro.Schema
import org.apache.avro.generic.GenericFixed
import java.nio.ByteBuffer

interface StructureEncoder : FieldEncoder {

   override fun beginStructure(desc: SerialDescriptor, vararg typeParams: KSerializer<*>): CompositeEncoder {
      return when (desc.kind) {
         StructureKind.LIST -> {
            when (desc.getElementDescriptor(0).kind) {
               PrimitiveKind.BYTE -> ByteArrayEncoder(fieldSchema(), context) { addValue(it) }
               else -> ListEncoder(fieldSchema(), context) { addValue(it) }
            }
         }
         StructureKind.CLASS -> RecordEncoder(fieldSchema(), context, desc) { addValue(it) }
         StructureKind.MAP -> MapEncoder(fieldSchema(), context, desc) { addValue(it) }
         else -> throw SerializationException(".beginStructure was called on a non-structure type [$desc]")
      }
   }
}

class RecordEncoder(private val schema: Schema,
                    override val context: SerialModule,
                    private val desc: SerialDescriptor,
                    val callback: (Record) -> Unit) : ElementValueEncoder(), StructureEncoder {

   private val builder = RecordBuilder(schema)
   private var currentIndex = -1

   override fun fieldSchema(): Schema = schema.fields[currentIndex].schema()

   override fun addValue(value: Any) {
      builder.add(value)
   }

   override fun encodeString(value: String) {
      builder.add(StringToAvroValue.toValue(fieldSchema(), value))
   }

   override fun encodeValue(value: Any) {
      builder.add(value)
   }

   override fun encodeElement(desc: SerialDescriptor, index: Int): Boolean {
      currentIndex = index
      return true
   }

   override fun encodeByteArray(buffer: ByteBuffer) {
      builder.add(buffer)
   }

   override fun encodeFixed(fixed: GenericFixed) {
      builder.add(fixed)
   }

   override fun encodeEnum(enumDescription: EnumDescriptor, ordinal: Int) {
      builder.add(ValueToEnum.toValue(fieldSchema(), enumDescription, ordinal))
   }

   override fun beginStructure(desc: SerialDescriptor, vararg typeParams: KSerializer<*>): CompositeEncoder {
      return super<StructureEncoder>.beginStructure(desc, *typeParams)
   }

   override fun endStructure(desc: SerialDescriptor) {
      callback(builder.record())
   }

   override fun encodeNull() {
      builder.add(null)
   }
}

class RecordBuilder(private val schema: Schema) {

   private val values = arrayListOf<Any?>()

   fun add(value: Any?) = values.add(value)

   fun record(): Record = ListRecord(schema, values)
}