package com.sksamuel.avro4k.decoder

import kotlinx.serialization.CompositeDecoder.Companion.READ_DONE
import kotlinx.serialization.SerialDescriptor
import kotlinx.serialization.builtins.AbstractDecoder

class InlineDecoder(private val value: Any?) : AbstractDecoder() {
   private var index = -1
   override fun decodeElementIndex(descriptor: SerialDescriptor): Int = if(++index < 1) index else READ_DONE

   override fun decodeString(): String {
      return StringFromAvroValue.fromValue(value)
   }

   override fun decodeValue(): Any {
      return value!!
   }
}