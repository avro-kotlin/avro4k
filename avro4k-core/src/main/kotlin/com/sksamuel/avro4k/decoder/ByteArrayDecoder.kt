package com.sksamuel.avro4k.decoder

import kotlinx.serialization.CompositeDecoder.Companion.READ_DONE
import kotlinx.serialization.SerialDescriptor
import kotlinx.serialization.builtins.AbstractDecoder

class ByteArrayDecoder(val data: ByteArray) : AbstractDecoder() {

   private var index = -1

   override fun decodeCollectionSize(descriptor: SerialDescriptor): Int = data.size
   override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
      index++
      return if(index < data.size) index else READ_DONE
   }

   override fun decodeByte(): Byte {
      return data[index]
   }
}