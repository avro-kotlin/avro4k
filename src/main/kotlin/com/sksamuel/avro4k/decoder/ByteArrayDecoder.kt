package com.sksamuel.avro4k.decoder

import kotlinx.serialization.ElementValueDecoder
import kotlinx.serialization.SerialDescriptor

class ByteArrayDecoder(val data: ByteArray) : ElementValueDecoder() {

   private var index = 0

   override fun decodeCollectionSize(desc: SerialDescriptor): Int = data.size

   override fun decodeByte(): Byte {
      return data[index++]
   }
}