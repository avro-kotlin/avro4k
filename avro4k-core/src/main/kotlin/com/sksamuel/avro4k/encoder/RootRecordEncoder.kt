package com.sksamuel.avro4k.encoder

import com.sksamuel.avro4k.Record
import kotlinx.serialization.*
import kotlinx.serialization.builtins.AbstractEncoder
import kotlinx.serialization.modules.SerialModule
import org.apache.avro.Schema

class RootRecordEncoder(private val schema: Schema,
                        override val context: SerialModule,
                        private val callback: (Record) -> Unit) : AbstractEncoder() {

   override fun beginStructure(descriptor: SerialDescriptor, vararg typeSerializers: KSerializer<*>): CompositeEncoder {
      return when (descriptor.kind) {
         is StructureKind.CLASS -> RecordEncoder(schema, context, descriptor, callback)
         else -> throw SerializationException("Unsupported root element passed to root record encoder")
      }
   }

   override fun endStructure(descriptor: SerialDescriptor) {

   }
}