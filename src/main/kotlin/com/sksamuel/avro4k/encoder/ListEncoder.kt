package com.sksamuel.avro4k.encoder

import kotlinx.serialization.CompositeEncoder
import kotlinx.serialization.ElementValueEncoder
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerialDescriptor
import kotlinx.serialization.internal.EnumDescriptor
import kotlinx.serialization.modules.SerialModule
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

class ListEncoder(private val schema: Schema,
                  override val context: SerialModule,
                  private val callback: (GenericData.Array<Any?>) -> Unit) : ElementValueEncoder(), StructureEncoder {

   private val list = mutableListOf<Any?>()

   override fun endStructure(desc: SerialDescriptor) {
      val generic = GenericData.Array(schema, list.toList())
      callback(generic)
   }

   override fun fieldSchema(): Schema = schema.elementType

   override fun beginStructure(desc: SerialDescriptor, vararg typeParams: KSerializer<*>): CompositeEncoder {
      return super<StructureEncoder>.beginStructure(desc, *typeParams)
   }

   override fun addValue(value: Any) {
      list.add(value)
   }

   override fun encodeString(value: String) {
      list.add(StringToAvroValue.toValue(schema, value))
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

   override fun encodeEnum(enumDescription: EnumDescriptor, ordinal: Int) {
      list.add(ValueToEnum.toValue(fieldSchema(), enumDescription, ordinal))
   }
}