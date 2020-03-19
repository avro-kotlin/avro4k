package com.sksamuel.avro4k.encoder

import com.sksamuel.avro4k.Record
import com.sksamuel.avro4k.RecordNaming
import kotlinx.serialization.*
import kotlinx.serialization.builtins.AbstractEncoder
import kotlinx.serialization.modules.SerialModule
import org.apache.avro.Schema

class UnionEncoder(private val unionSchema : Schema,
                   override val context: SerialModule,
                   private val callback: (Record) -> Unit) : AbstractEncoder() {

   override fun beginStructure(descriptor: SerialDescriptor, vararg typeSerializers: KSerializer<*>): CompositeEncoder {
      return when (descriptor.kind) {
         is StructureKind.CLASS -> {
            //Hand in the concrete schema for the specified SerialDescriptor so that fields can be correctly decoded.
            val leafSchema = unionSchema.types.first{
               val schemaName = RecordNaming(it.fullName, emptyList())
               val serialName = RecordNaming(descriptor)
               serialName.name() == schemaName.name() && serialName.namespace() == schemaName.namespace()
            }
            RecordEncoder(leafSchema, context, descriptor, callback)
         }
         else -> throw SerializationException("Unsupported root element passed to root record encoder")
      }
   }

   override fun endStructure(descriptor: SerialDescriptor) {

   }
}