package com.sksamuel.avro4k.encoder

import com.sksamuel.avro4k.ListRecord
import com.sksamuel.avro4k.Record
import com.sksamuel.avro4k.serializer.BigDecimalEncoder
import kotlinx.serialization.CompositeEncoder
import kotlinx.serialization.ElementValueEncoder
import kotlinx.serialization.KSerializer
import kotlinx.serialization.PrimitiveKind
import kotlinx.serialization.SerialDescriptor
import kotlinx.serialization.StructureKind
import kotlinx.serialization.internal.EnumDescriptor
import kotlinx.serialization.modules.SerialModule
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

class RecordEncoder(private val schema: Schema,
                    override val context: SerialModule,
                    val callback: (Record) -> Unit) : ElementValueEncoder(), BigDecimalEncoder {

   private val builder = RecordBuilder(schema)
   private var currentIndex = -1

   override fun fieldSchema(): Schema = schema.fields[currentIndex].schema()

   override fun addValue(value: Any) {
      builder.add(value)
   }

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
               PrimitiveKind.BYTE -> ByteArrayEncoder(fieldSchema(), context) {
                  builder.add(it)
               }
               else -> ListEncoder(fieldSchema(), context) { builder.add(it) }
            }
         }
         StructureKind.CLASS -> RecordEncoder(fieldSchema(), context) { builder.add(it) }
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

class RecordBuilder(private val schema: Schema) {

   private val values = arrayListOf<Any?>()

   fun add(value: Any?) = values.add(value)

   fun record(): Record = ListRecord(schema, values)
}