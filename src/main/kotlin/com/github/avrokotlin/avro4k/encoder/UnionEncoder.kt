package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.Record
import com.github.avrokotlin.avro4k.RecordNaming
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
                   private val callback: (Record) -> Unit) : AbstractEncoder() {

   override fun beginStructure(descriptor: SerialDescriptor): CompositeEncoder {
      return when (descriptor.kind) {
         is StructureKind.CLASS, is StructureKind.OBJECT -> {
            //Hand in the concrete schema for the specified SerialDescriptor so that fields can be correctly decoded.
            val leafSchema = unionSchema.types.first{
               val schemaName = RecordNaming(it.fullName, emptyList())
               val serialName = RecordNaming(descriptor)
               serialName == schemaName
            }
            RecordEncoder(leafSchema, serializersModule, callback)
         }
         else -> throw SerializationException("Unsupported root element passed to root record encoder")
      }
   }

   override fun endStructure(descriptor: SerialDescriptor) {

   }
}