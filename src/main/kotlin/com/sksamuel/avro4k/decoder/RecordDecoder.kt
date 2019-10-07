package com.sksamuel.avro4k.decoder

import com.sksamuel.avro4k.FieldNaming
import com.sksamuel.avro4k.serializer.BigDecimalDecoder
import kotlinx.serialization.CompositeDecoder
import kotlinx.serialization.ElementValueDecoder
import kotlinx.serialization.KSerializer
import kotlinx.serialization.PrimitiveKind
import kotlinx.serialization.SerialDescriptor
import kotlinx.serialization.SerializationException
import kotlinx.serialization.StructureKind
import kotlinx.serialization.internal.EnumDescriptor
import org.apache.avro.Conversions
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import org.apache.avro.generic.GenericFixed
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import java.math.BigDecimal
import java.nio.ByteBuffer

class RecordDecoder(private val desc: SerialDescriptor,
                    private val record: GenericRecord) : ElementValueDecoder(), BigDecimalDecoder {

   private var currentIndex = -1

   @Suppress("UNCHECKED_CAST")
   override fun beginStructure(desc: SerialDescriptor, vararg typeParams: KSerializer<*>): CompositeDecoder {
      return when (desc.kind as StructureKind) {
         StructureKind.CLASS -> RecordDecoder(desc, fieldValue() as GenericRecord)
         StructureKind.MAP -> MapDecoder(desc, fieldSchema(), fieldValue() as Map<String, *>)
         StructureKind.LIST -> {
            val decoder: CompositeDecoder = if (desc.getElementDescriptor(1).kind == PrimitiveKind.BYTE) {
               when (val data = fieldValue()) {
                  is List<*> -> ByteArrayDecoder((data as List<Byte>).toByteArray())
                  is Array<*> -> ByteArrayDecoder((data as Array<Byte>).toByteArray())
                  is ByteArray -> ByteArrayDecoder(data)
                  is ByteBuffer -> ByteArrayDecoder(data.array())
                  else -> this
               }
            } else {
               when (val data = fieldValue()) {
                  is List<*> -> ListDecoder(fieldSchema(), data)
                  is Array<*> -> ListDecoder(fieldSchema(), data.asList())
                  else -> this
               }
            }
            decoder
         }
      }
   }

   private fun fieldValue(): Any? = record[resolvedFieldName()]

   private fun resolvedFieldName(): String = FieldNaming(desc, currentIndex).name()

   private fun field(): Schema.Field = record.schema.getField(resolvedFieldName())

   private fun fieldSchema(): Schema = field().schema()

   override fun decodeString(): String = StringFromAvroValue.fromValue(fieldValue())

   override fun decodeBigDecimal(): BigDecimal {

      val fieldSchema = fieldSchema()

      fun logical() = when (val l = fieldSchema.logicalType) {
         is LogicalTypes.Decimal -> l
         else -> throw SerializationException("Cannot decode to BigDecimal when field schema [$fieldSchema] does not define Decimal logical type [$l]")
      }

      return when (val v = fieldValue()) {
         is Utf8 -> BigDecimal(v.toString())
         is ByteArray -> Conversions.DecimalConversion().fromBytes(ByteBuffer.wrap(v), fieldSchema, logical())
         is ByteBuffer -> Conversions.DecimalConversion().fromBytes(v, fieldSchema, logical())
         is GenericFixed -> Conversions.DecimalConversion().fromFixed(v, fieldSchema, logical())
         else -> throw SerializationException("Unsupported BigDecimal type [$v]")
      }
   }

   override fun decodeBoolean(): Boolean {
      return when (val v = fieldValue()) {
         is Boolean -> v
         null -> throw SerializationException("Cannot decode <null> as a Boolean")
         else -> throw SerializationException("Unsupported type for Boolean ${v.javaClass}")
      }
   }

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

   override fun decodeEnum(enumDescription: EnumDescriptor): Int {
      val symbol = EnumFromAvroValue.fromValue(fieldValue()!!)
      return (0 until enumDescription.elementsCount).find { enumDescription.getElementName(it) == symbol } ?: -1
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
         else -> throw SerializationException("Unsupported type for Long ${v.javaClass}")
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

   override fun decodeElementIndex(desc: SerialDescriptor): Int {
      currentIndex++
      while (currentIndex < desc.elementsCount) {
         if (desc.isElementOptional(currentIndex)) {
            currentIndex++
         } else {
            return currentIndex
         }
      }
      return CompositeDecoder.READ_DONE
   }
}

