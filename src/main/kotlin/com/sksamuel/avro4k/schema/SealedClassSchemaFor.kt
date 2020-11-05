package com.sksamuel.avro4k.schema

import com.sksamuel.avro4k.RecordNaming
import com.sksamuel.avro4k.leavesOfSealedClasses
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema

@ExperimentalSerializationApi
class SealedClassSchemaFor(private val descriptor: SerialDescriptor,
                           private val namingStrategy: NamingStrategy,
                           private val serializersModule: SerializersModule,
                           private val resolvedSchemas: MutableMap<RecordNaming, Schema>
) : SchemaFor {
   override fun schema(): Schema {
      val leafSerialDescriptors = descriptor.leavesOfSealedClasses()
      return Schema.createUnion(
         leafSerialDescriptors.map { ClassSchemaFor(it,namingStrategy,serializersModule, resolvedSchemas).schema() }
      )
   }
}
