package com.sksamuel.avro4k.decoder

import kotlinx.serialization.*
import kotlinx.serialization.CompositeDecoder.Companion.READ_DONE
import kotlinx.serialization.builtins.AbstractDecoder
import org.apache.avro.generic.GenericRecord

class RootRecordDecoder(private val record: GenericRecord) : AbstractDecoder() {
   var decoded = false
   override fun beginStructure(descriptor: SerialDescriptor, vararg typeParams: KSerializer<*>): CompositeDecoder {
      return when (descriptor.kind) {
         StructureKind.CLASS -> RecordDecoder(descriptor, record)
         else -> throw SerializationException("Non-class structure passed to root record decoder")
      }
   }

   override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
      val index = if(decoded) READ_DONE else 0
      decoded = true
      return index
   }
}