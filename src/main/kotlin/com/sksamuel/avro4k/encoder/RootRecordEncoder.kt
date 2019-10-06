package com.sksamuel.avro4k.encoder

import com.sksamuel.avro4k.Record
import kotlinx.serialization.CompositeEncoder
import kotlinx.serialization.ElementValueEncoder
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerialDescriptor
import kotlinx.serialization.SerializationException
import kotlinx.serialization.StructureKind
import kotlinx.serialization.modules.SerialModule
import org.apache.avro.Schema

class RootRecordEncoder(private val schema: Schema,
                        override val context: SerialModule,
                        private val callback: (Record) -> Unit) : ElementValueEncoder() {

   override fun beginStructure(desc: SerialDescriptor, vararg typeParams: KSerializer<*>): CompositeEncoder {
      return when (desc.kind) {
         is StructureKind.CLASS -> RecordEncoder(schema, context, desc, callback)
         else -> throw SerializationException("Unsupported root element passed to root record encoder")
      }
   }
}