package com.sksamuel.avro4k.decoder

import kotlinx.serialization.ElementValueDecoder

class InlineDecoder(private val value: Any?) : ElementValueDecoder() {

   override fun decodeString(): String {
      return StringFromAvroValue.fromValue(value)
   }

   override fun decodeValue(): Any {
      return value!!
   }
}