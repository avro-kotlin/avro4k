package com.github.avrokotlin.avro4k.encoder.direct


import com.github.avrokotlin.avro4k.io.AvroEncoder
import com.github.avrokotlin.avro4k.schema.nonNullSchema
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.CompositeEncoder
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema

@ExperimentalSerializationApi
class MapEncoder(schema: Schema,
                 override val serializersModule: SerializersModule,
                 override val avroEncoder: AvroEncoder) : StructureEncoder(),
   CompositeEncoder {
   
   private var key: String? = null
   private val valueSchema = schema.valueType
   override fun encodeString(value: String) {
      val k = key
      if (k == null) {
         key = value
         avroEncoder.startItem()
         avroEncoder.writeString(value)
      } else {
         writeNonNullValue { avroEncoder.writeString(valueSchema.nonNullSchema, value) }
      }
   }

   override fun writeNonNullValue(doWriteValue: () -> Unit) {
      super.writeNonNullValue(doWriteValue)
      key = null
   }
   override fun endStructure(descriptor: SerialDescriptor) {
      avroEncoder.writeMapEnd()
   }

   override fun beginCollection(descriptor: SerialDescriptor, collectionSize: Int): CompositeEncoder {
      avroEncoder.writeMapStart(collectionSize)
      return beginStructure(descriptor)
   }

   override fun beginStructure(descriptor: SerialDescriptor): CompositeEncoder {
      val result = super.beginStructure(descriptor)
      key = null
      return result
   }

   override fun fieldSchema(): Schema = valueSchema
}
