package com.sksamuel.avro4k.decoder

import com.sksamuel.avro4k.AnnotationExtractor
import com.sksamuel.avro4k.FieldNaming
import com.sksamuel.avro4k.schema.extractNonNull
import kotlinx.serialization.*
import kotlinx.serialization.builtins.AbstractDecoder
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import java.nio.ByteBuffer

interface ExtendedDecoder : Decoder {
   fun decodeAny(): Any?
}

interface FieldDecoder : ExtendedDecoder {
   fun fieldSchema(): Schema
}

class RecordDecoder(private val desc: SerialDescriptor,
                    private val record: GenericRecord) : AbstractDecoder(), FieldDecoder {

   private var currentIndex = -1

   @Suppress("UNCHECKED_CAST")
   override fun beginStructure(descriptor: SerialDescriptor, vararg typeParams: KSerializer<*>): CompositeDecoder {
      val valueType = AnnotationExtractor(descriptor.annotations).valueType()
      val value = fieldValue()
      return when (descriptor.kind) {
         StructureKind.CLASS ->
            if (valueType)
               InlineDecoder(fieldValue())
            else
               RecordDecoder(descriptor, value as GenericRecord)
         StructureKind.MAP -> MapDecoder(descriptor, fieldSchema(), value as Map<String, *>)
         StructureKind.LIST -> {
            val decoder: CompositeDecoder = if (descriptor.getElementDescriptor(0).kind == PrimitiveKind.BYTE) {
               when (value) {
                  is List<*> -> ByteArrayDecoder((value as List<Byte>).toByteArray())
                  is Array<*> -> ByteArrayDecoder((value as Array<Byte>).toByteArray())
                  is ByteArray -> ByteArrayDecoder(value)
                  is ByteBuffer -> ByteArrayDecoder(value.array())
                  else -> this
               }
            } else {
               when (value) {
                  is List<*> -> ListDecoder(fieldSchema(), value)
                  is Array<*> -> ListDecoder(fieldSchema(), value.asList())
                  else -> this
               }
            }
            decoder
         }
         PolymorphicKind.SEALED -> SealedClassDecoder(descriptor,value as GenericRecord)
         else -> throw UnsupportedOperationException("Decoding descriptor of kind ${descriptor.kind} is currently not supported")
      }
   }

   private fun fieldValue(): Any? = record[resolvedFieldName()]

   private fun resolvedFieldName(): String = FieldNaming(desc, currentIndex).name()

   private fun field(): Schema.Field = record.schema.getField(resolvedFieldName())

   override fun fieldSchema(): Schema {
      // if the element is nullable, then we should have a union schema which we can extract the non-null schema from
      return if (desc.getElementDescriptor(currentIndex).isNullable) {
         field().schema().extractNonNull()
      } else {
         field().schema()
      }
   }

   override fun decodeString(): String = StringFromAvroValue.fromValue(fieldValue())

   override fun decodeBoolean(): Boolean {
      return when (val v = fieldValue()) {
         is Boolean -> v
         null -> throw SerializationException("Cannot decode <null> as a Boolean")
         else -> throw SerializationException("Unsupported type for Boolean ${v.javaClass}")
      }
   }

   override fun decodeAny(): Any? = fieldValue()

   override fun decodeByte(): Byte {
      return when (val v = fieldValue()) {
         is Byte -> v
         is Int -> if (v < 255) v.toByte() else throw SerializationException("Out of bound integer cannot be converted to byte [$v]")
         null -> throw SerializationException("Cannot decode <null> as a Byte")
         else -> throw SerializationException("Unsupported type for Byte ${v.javaClass}")
      }
   }

   override fun decodeNotNullMark(): Boolean {
      return fieldValue() != null
   }

   override fun decodeEnum(enumDescriptor: SerialDescriptor): Int {
      val symbol = EnumFromAvroValue.fromValue(fieldValue()!!)
      return (0 until enumDescriptor.elementsCount).find { enumDescriptor.getElementName(it) == symbol } ?: -1
   }

   override fun decodeFloat(): Float {
      return when (val v = fieldValue()) {
         is Float -> v
         null -> throw SerializationException("Cannot decode <null> as a Float")
         else -> throw SerializationException("Unsupported type for Float ${v.javaClass}")
      }
   }

   override fun decodeInt(): Int {
      return when (val v = fieldValue()) {
         is Int -> v
         null -> throw SerializationException("Cannot decode <null> as a Int")
         else -> throw SerializationException("Unsupported type for Int ${v.javaClass}")
      }
   }

   override fun decodeLong(): Long {
      return when (val v = fieldValue()) {
         is Long -> v
         is Int -> v.toLong()
         null -> throw SerializationException("Cannot decode <null> as a Long")
         else -> throw SerializationException("Unsupported type for Long [is ${v.javaClass}]")
      }
   }

   override fun decodeDouble(): Double {
      return when (val v = fieldValue()) {
         is Double -> v
         is Float -> v.toDouble()
         null -> throw SerializationException("Cannot decode <null> as a Double")
         else -> throw SerializationException("Unsupported type for Double ${v.javaClass}")
      }
   }

   override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
      currentIndex++
      return if (currentIndex < descriptor.elementsCount) currentIndex else CompositeDecoder.READ_DONE
   }
}

