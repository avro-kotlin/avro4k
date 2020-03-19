package com.sksamuel.avro4k.schema

import com.sksamuel.avro4k.leafsOfSealedClasses
import kotlinx.serialization.SerialDescriptor
import kotlinx.serialization.modules.SerialModule
import org.apache.avro.Schema

class SealedClassSchemaFor(private val descriptor: SerialDescriptor,
                           private val namingStrategy: NamingStrategy,
                           private val context: SerialModule
) : SchemaFor {
   override fun schema(): Schema {
      val leafSerialDescriptors = descriptor.leafsOfSealedClasses()
      return Schema.createUnion(
         leafSerialDescriptors.map { ClassSchemaFor(it,namingStrategy,context).schema() }
      )
   }
}
