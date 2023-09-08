package com.github.avrokotlin.avro4k.encoder.direct

import com.github.avrokotlin.avro4k.io.AvroEncoder
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.PolymorphicKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.encoding.AbstractEncoder
import kotlinx.serialization.encoding.CompositeEncoder
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema

@ExperimentalSerializationApi
class DirectRootRecordEncoder(private val schema: Schema,
                              override val serializersModule: SerializersModule,
                              private val avroEncoder: AvroEncoder) : AbstractEncoder() {

   override fun beginStructure(descriptor: SerialDescriptor): CompositeEncoder {
      return when (descriptor.kind) {
         is StructureKind.CLASS -> RecordEncoder(schema, serializersModule, avroEncoder)
         is PolymorphicKind -> UnionEncoder(schema,serializersModule, avroEncoder)
         else -> throw SerializationException("Unsupported root element passed to root record encoder")
      }
   }

   override fun endStructure(descriptor: SerialDescriptor) {

   }
}