package com.github.avrokotlin.avro4k.decoder

import com.github.avrokotlin.avro4k.schema.NamingStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.PolymorphicKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.encoding.AbstractDecoder
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.encoding.CompositeDecoder.Companion.DECODE_DONE
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.generic.GenericRecord

@ExperimentalSerializationApi
class RootRecordDecoder(
   private val record: GenericRecord,
   override val serializersModule: SerializersModule,
   private val namingStrategy: NamingStrategy,
) : AbstractDecoder() {
   var decoded = false
   override fun beginStructure(descriptor: SerialDescriptor): CompositeDecoder {
      return when (descriptor.kind) {
         StructureKind.CLASS, StructureKind.OBJECT -> RecordDecoder(
            descriptor,
            record,
            serializersModule,
            namingStrategy
         )
         PolymorphicKind.SEALED -> SealedClassDecoder(descriptor, record, serializersModule, namingStrategy)
         else -> throw SerializationException("Non-class structure passed to root record decoder")
      }
   }

   override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
      val index = if(decoded) DECODE_DONE else 0
      decoded = true
      return index
   }
}
