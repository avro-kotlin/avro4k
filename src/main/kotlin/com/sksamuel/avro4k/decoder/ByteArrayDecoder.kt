package com.sksamuel.avro4k.decoder

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.AbstractDecoder
import kotlinx.serialization.encoding.CompositeDecoder.Companion.DECODE_DONE

@ExperimentalSerializationApi
class ByteArrayDecoder(val data: ByteArray) : AbstractDecoder() {

   private var index = -1

   override fun decodeCollectionSize(descriptor: SerialDescriptor): Int = data.size
   override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
      index++
      return if(index < data.size) index else DECODE_DONE
   }

   override fun decodeByte(): Byte {
      return data[index]
   }
}