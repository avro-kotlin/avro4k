package com.sksamuel.avro4k.schema

import kotlinx.serialization.SerialDescriptor
import org.apache.avro.Schema

class SealedClassSchemaFor(private val descriptor: SerialDescriptor) : SchemaFor {
   override fun schema(namingStrategy: NamingStrategy): Schema {
      TODO()
   }
}
