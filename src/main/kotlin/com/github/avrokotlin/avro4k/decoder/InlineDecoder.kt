package com.github.avrokotlin.avro4k.decoder

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.AbstractDecoder
import kotlinx.serialization.encoding.CompositeDecoder.Companion.DECODE_DONE
import kotlinx.serialization.modules.SerializersModule

@ExperimentalSerializationApi
class InlineDecoder(private val value: Any?, override val serializersModule: SerializersModule) : AbstractDecoder() {
   private var index = -1
   override fun decodeElementIndex(descriptor: SerialDescriptor): Int = if(++index < 1) index else DECODE_DONE

   override fun decodeString(): String {
      return StringFromAvroValue.fromValue(value)
   }

   override fun decodeValue(): Any {
      return value!!
   }
}