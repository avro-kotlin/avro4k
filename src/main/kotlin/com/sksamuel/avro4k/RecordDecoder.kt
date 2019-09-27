package com.sksamuel.avro4k

import kotlinx.serialization.CompositeDecoder
import kotlinx.serialization.ElementValueDecoder
import kotlinx.serialization.KSerializer
import kotlinx.serialization.NamedValueDecoder
import kotlinx.serialization.PrimitiveKind
import kotlinx.serialization.SerialDescriptor
import kotlinx.serialization.SerializationException
import kotlinx.serialization.StructureKind
import kotlinx.serialization.internal.EnumDescriptor
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericEnumSymbol
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import java.nio.ByteBuffer

class RecordDecoder(val record: GenericRecord) : NamedValueDecoder() {

   private var currentIndex = 0
   private var currentDesc: SerialDescriptor? = null

   override fun beginStructure(desc: SerialDescriptor, vararg typeParams: KSerializer<*>): CompositeDecoder {
      println("beginStructure $desc")
      if (currentTagOrNull == null) {
         currentDesc = desc
         return this
      }
      return when (desc.kind as StructureKind) {
         // if we have a class and the current tag is null, then we are in the "root" class and just use "this" decoder
         // otherwise we'll recurse into a fresh ClassDecoder
         StructureKind.CLASS -> RecordDecoder(record.get(currentTag) as GenericRecord)
         StructureKind.MAP -> this
         StructureKind.LIST -> {
            val decoder: CompositeDecoder = if (desc.getElementDescriptor(1).kind == PrimitiveKind.BYTE) {
               when (val data = record.get(currentTag)) {
                  is List<*> -> ByteArrayDecoder((data as List<Byte>).toByteArray())
                  is Array<*> -> ByteArrayDecoder((data as Array<Byte>).toByteArray())
                  is ByteArray -> ByteArrayDecoder(data)
                  is ByteBuffer -> ByteArrayDecoder(data.array())
                  else -> this
               }
            } else {
               when (val data = record.get(currentTag)) {
                  is List<*> -> ListDecoder(data)
                  is Array<*> -> ListDecoder(data.asList())
                  else -> this
               }
            }
            decoder
         }
      }
   }

   private fun resolvedName(): String {
      // the tag is the name of the field in the data class, but the record can have the field
      // stored under another name defined by @AvroName
      // so we must look up the name defined by the annotation from the descriptor annotations
      val naming = NameExtractor(currentDesc!!, currentIndex - 1)
      return naming.name()
   }

   override fun decodeTaggedDouble(tag: String): Double {
      return record.get(resolvedName()) as Double
   }

   override fun decodeTaggedLong(tag: String): Long {
      return record.get(resolvedName()) as Long
   }

   override fun decodeTaggedFloat(tag: String): Float {
      return record.get(resolvedName()) as Float
   }

   override fun decodeTaggedBoolean(tag: String): Boolean {
      return record.get(resolvedName()) as Boolean
   }

   override fun decodeTaggedInt(tag: String): Int {
      return record.get(resolvedName()) as Int
   }

   override fun decodeTaggedEnum(tag: String, enumDescription: EnumDescriptor): Int {
      val symbol = when (val v = record.get(resolvedName())) {
         is GenericEnumSymbol<*> -> v.toString()
         is String -> v
         else -> v.toString()
      }
      return (0 until enumDescription.elementsCount).find { enumDescription.getElementName(it) == symbol } ?: -1
   }

   override fun decodeTaggedString(tag: String): String {
      return when (val v = record.get(resolvedName())) {
         is String -> v
         is Utf8 -> v.toString()
         is GenericData.Fixed -> String(v.bytes())
         is ByteArray -> String(v)
         is CharSequence -> v.toString()
         is ByteBuffer -> String(v.array())
         null -> throw SerializationException("Cannot decode <null> as a string")
         else -> throw SerializationException("Unsupported type for String ${v.javaClass}")
      }
   }

   override fun decodeTaggedNotNullMark(tag: String): Boolean {
      return record.get(tag) != null
   }

   override fun decodeElementIndex(desc: SerialDescriptor): Int {
      println("decodeElementIndex $desc $currentIndex")
      while (currentIndex < desc.elementsCount) {
         if (desc.isElementOptional(currentIndex)) {
            currentIndex++
         } else {
            val k = currentIndex
            currentIndex++
            return k
         }
      }
      return CompositeDecoder.READ_DONE
   }
}

class ByteArrayDecoder(val data: ByteArray) : ElementValueDecoder() {

   private var index = 0

   override fun decodeCollectionSize(desc: SerialDescriptor): Int = data.size

   override fun decodeByte(): Byte {
      return data[index++]
   }
}

class ListDecoder(private val array: List<Any?>) : ElementValueDecoder() {

   private var index = 0

   init {
      println(array)
   }

   override fun decodeBoolean(): Boolean {
      return array[index++] as Boolean
   }

   override fun decodeLong(): Long {
      return array[index++] as Long
   }

   override fun decodeString(): String {
      return array[index++] as String
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

   override fun beginStructure(desc: SerialDescriptor, vararg typeParams: KSerializer<*>): CompositeDecoder {
      println("beginStructure $desc")
      return when (desc.kind as StructureKind) {
         StructureKind.CLASS -> RecordDecoder(array[index++] as GenericRecord)
         StructureKind.MAP, StructureKind.LIST -> this
      }
   }

   override fun decodeCollectionSize(desc: SerialDescriptor): Int = array.size
}