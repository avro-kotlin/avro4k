package com.github.avrokotlin.avro4k.encoder.direct

import com.github.avrokotlin.avro4k.RecordNaming
import com.github.avrokotlin.avro4k.io.AvroEncoder
import com.github.avrokotlin.avro4k.schema.DefaultNamingStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.encoding.AbstractEncoder
import kotlinx.serialization.encoding.CompositeEncoder
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema

@ExperimentalSerializationApi
class UnionEncoder(private val unionSchema : Schema,
                   override val serializersModule: SerializersModule,
                   private val avroEncoder: AvroEncoder) : AbstractEncoder() {
   override fun encodeString(value: String){
      //No need to encode the name of the concrete type. The name will never be encoded in the avro schema.
   }
   override fun beginStructure(descriptor: SerialDescriptor): CompositeEncoder {
      return when (descriptor.kind) {
         is StructureKind.CLASS, is StructureKind.OBJECT -> {
            //Hand in the concrete schema for the specified SerialDescriptor so that fields can be correctly decoded.
            val serialName = RecordNaming(descriptor, DefaultNamingStrategy)
            val leafSchemaIndex = unionSchema.types.indexOfFirst{
               val schemaName = RecordNaming(it.fullName, emptyList(), DefaultNamingStrategy)               
               serialName == schemaName
            }
            val leafSchema = unionSchema.types[leafSchemaIndex]
            avroEncoder.writeUnionSchema(unionSchema, leafSchemaIndex)
            RecordEncoder(leafSchema, serializersModule, avroEncoder)
         }
         else -> throw SerializationException("Unsupported root element passed to union encoder")
      }
   }

   override fun endStructure(descriptor: SerialDescriptor) {
      //no op
   }
}