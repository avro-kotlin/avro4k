package com.sksamuel.avro4k.decoder

import kotlinx.serialization.CompositeDecoder
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ElementValueDecoder
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerialDescriptor
import kotlinx.serialization.StructureKind
import org.apache.avro.Schema
import org.apache.avro.generic.GenericArray
import org.apache.avro.generic.GenericRecord

class ListDecoder(private val schema: Schema,
                  private val array: List<Any?>) : ElementValueDecoder(), FieldDecoder {

   init {
      require(schema.type == Schema.Type.ARRAY)
   }

   private var index = 0

   override fun decodeBoolean(): Boolean {
      return array[index++] as Boolean
   }

   override fun decodeLong(): Long {
      return array[index++] as Long
   }

   override fun decodeString(): String {
      val raw = array[index++]
      return StringFromAvroValue.fromValue(raw)
   }

   override fun decodeDouble(): Double {
      return array[index++] as Double
   }

   override fun decodeFloat(): Float {
      return array[index++] as Float
   }

   override fun decodeByte(): Byte {
      return array[index++] as Byte
   }

   override fun decodeInt(): Int {
      return array[index++] as Int
   }

   override fun decodeChar(): Char {
      return array[index++] as Char
   }

   override fun decodeAny(): Any? {
      return array[index++]
   }

   override fun fieldSchema(): Schema = schema.elementType

   override fun decodeEnum(enumDescription: SerialDescriptor): Int {
      val symbol = EnumFromAvroValue.fromValue(array[index++]!!)
      return (0 until enumDescription.elementsCount).find { enumDescription.getElementName(it) == symbol } ?: -1
   }

   override fun <T> decodeSerializableValue(deserializer: DeserializationStrategy<T>): T {
      return deserializer.deserialize(this)
   }

   @Suppress("UNCHECKED_CAST")
   override fun beginStructure(desc: SerialDescriptor, vararg typeParams: KSerializer<*>): CompositeDecoder {
      return when (desc.kind as StructureKind) {
         StructureKind.CLASS -> RecordDecoder(desc, array[index++] as GenericRecord)
         StructureKind.LIST -> ListDecoder(schema.elementType, array[index++] as GenericArray<*>)
         StructureKind.MAP -> MapDecoder(desc, schema.elementType, array[index++] as Map<String, *>)
      }
   }

   override fun decodeCollectionSize(desc: SerialDescriptor): Int = array.size
}