package com.github.avrokotlin.avro4k.decoder

import com.github.avrokotlin.avro4k.AvroConfiguration
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.encoding.AbstractDecoder
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema
import org.apache.avro.generic.GenericArray
import org.apache.avro.generic.GenericRecord
import java.nio.ByteBuffer

@ExperimentalSerializationApi
class MapDecoder(
   private val desc: SerialDescriptor,
   private val schema: Schema,
   map: Map<*, *>,
   override val serializersModule: SerializersModule,
   private val configuration: AvroConfiguration
) : AbstractDecoder(), CompositeDecoder {

   init {
      require(schema.type == Schema.Type.MAP)
   }

   private val entries = map.toList()
   private var index = -1
   private var key: String? = null

   override fun decodeString(): String {
      val entry = entries[index / 2]
      val value = when {
         index % 2 == 0 -> entry.first
         else -> entry.second
      }
      return StringFromAvroValue.fromValue(value)
   }

   private fun value(): Any? = entries[index / 2].second

   override fun decodeFloat(): Float {
      return when (val v = value()) {
         is Float -> v
         null -> throw SerializationException("Cannot decode <null> as a Float")
         else -> throw SerializationException("Unsupported type for Float ${v.javaClass}")
      }
   }

   override fun decodeInt(): Int {
      return when (val v = value()) {
         is Int -> v
         null -> throw SerializationException("Cannot decode <null> as a Int")
         else -> throw SerializationException("Unsupported type for Int ${v.javaClass}")
      }
   }

   override fun decodeLong(): Long {
      return when (val v = value()) {
         is Long -> v
         is Int -> v.toLong()
         null -> throw SerializationException("Cannot decode <null> as a Long")
         else -> throw SerializationException("Unsupported type for Long ${v.javaClass}")
      }
   }

   override fun decodeDouble(): Double {
      return when (val v = value()) {
         is Double -> v
         is Float -> v.toDouble()
         null -> throw SerializationException("Cannot decode <null> as a Double")
         else -> throw SerializationException("Unsupported type for Double ${v.javaClass}")
      }
   }

   override fun decodeByte(): Byte {
      return when (val v = value()) {
         is Byte -> v
         is Int -> v.toByte()
         null -> throw SerializationException("Cannot decode <null> as a Byte")
         else -> throw SerializationException("Unsupported type for Byte ${v.javaClass}")
      }
   }

   override fun decodeBoolean(): Boolean {
      return when (val v = value()) {
         is Boolean -> v
         null -> throw SerializationException("Cannot decode <null> as a Boolean")
         else -> throw SerializationException("Unsupported type for Boolean ${v.javaClass}")
      }
   }

   override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
      index++
      return if (index == entries.size * 2) CompositeDecoder.DECODE_DONE else index
   }

   @Suppress("UNCHECKED_CAST")
   override fun beginStructure(descriptor: SerialDescriptor): CompositeDecoder {
      return when (descriptor.kind as StructureKind) {
         StructureKind.CLASS -> RecordDecoder(descriptor, value() as GenericRecord, serializersModule, configuration)
         StructureKind.LIST -> when(descriptor.getElementDescriptor(0).kind) {
            PrimitiveKind.BYTE -> ByteArrayDecoder((value() as ByteBuffer).array(), serializersModule)
            else -> ListDecoder(schema.valueType, value() as GenericArray<*>, serializersModule, configuration)
         }
         StructureKind.MAP -> MapDecoder(descriptor, schema.valueType, value() as Map<String, *>, serializersModule, configuration)
         StructureKind.OBJECT -> throw UnsupportedOperationException("Objects are not supported right now as serialization kind")
      }
   }
}
