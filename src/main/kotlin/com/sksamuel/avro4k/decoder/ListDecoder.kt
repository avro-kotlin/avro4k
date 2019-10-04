package com.sksamuel.avro4k.decoder

import kotlinx.serialization.CompositeDecoder
import kotlinx.serialization.ElementValueDecoder
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerialDescriptor
import kotlinx.serialization.StructureKind
import kotlinx.serialization.internal.EnumDescriptor
import org.apache.avro.generic.GenericRecord

class ListDecoder(private val array: List<Any?>) : ElementValueDecoder() {

   private var index = 0

   override fun decodeBoolean(): Boolean {
      return array[index++] as Boolean
   }

   override fun decodeLong(): Long {
      return array[index++] as Long
   }

   override fun decodeString(): String = StringFromAvroValue.fromValue(array[index++])

   override fun decodeDouble(): Double {
      return array[index++] as Double
   }

   override fun decodeFloat(): Float {
      return array[index++] as Float
   }

   override fun decodeByte(): Byte {
      return array[index++] as Byte
   }

   override fun decodeEnum(enumDescription: EnumDescriptor): Int {
      val symbol = EnumFromAvroValue.fromValue(array[index++]!!)
      return (0 until enumDescription.elementsCount).find { enumDescription.getElementName(it) == symbol } ?: -1
   }

   override fun beginStructure(desc: SerialDescriptor, vararg typeParams: KSerializer<*>): CompositeDecoder {
      return when (desc.kind as StructureKind) {
         StructureKind.CLASS -> RecordDecoder(array[index++] as GenericRecord)
         StructureKind.MAP, StructureKind.LIST -> this
      }
   }

   override fun decodeCollectionSize(desc: SerialDescriptor): Int = array.size
}