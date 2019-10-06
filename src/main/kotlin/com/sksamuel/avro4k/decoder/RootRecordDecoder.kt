package com.sksamuel.avro4k.decoder

import kotlinx.serialization.CompositeDecoder
import kotlinx.serialization.ElementValueDecoder
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerialDescriptor
import kotlinx.serialization.SerializationException
import kotlinx.serialization.StructureKind
import org.apache.avro.generic.GenericRecord

class RootRecordDecoder(private val record: GenericRecord) : ElementValueDecoder() {

   override fun beginStructure(desc: SerialDescriptor, vararg typeParams: KSerializer<*>): CompositeDecoder {
      return when (desc.kind as StructureKind) {
         StructureKind.CLASS -> RecordDecoder(desc, record)
         else -> throw SerializationException("Non-class structure passed to root record decoder")
      }
   }
}