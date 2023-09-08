package com.github.avrokotlin.avro4k.encoder.direct

import com.github.avrokotlin.avro4k.io.AvroEncoder
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.CompositeEncoder
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema

@ExperimentalSerializationApi
class ListEncoder(private val schema: Schema,
                  override val serializersModule: SerializersModule,
                  override val avroEncoder: AvroEncoder) : StructureEncoder() {

   override fun beginCollection(descriptor: SerialDescriptor, collectionSize: Int): CompositeEncoder {
      avroEncoder.writeArrayStart(collectionSize)
      return super.beginStructure(descriptor)
   }

   override fun endStructure(descriptor: SerialDescriptor) {
      avroEncoder.writeArrayEnd()
   }

   override fun fieldSchema(): Schema = schema.elementType

   override fun writeNonNullValue(doWriteValue: () -> Unit) {
      avroEncoder.startItem()
      super.writeNonNullValue(doWriteValue)
   }
}