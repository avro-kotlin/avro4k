package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.RecordNaming
import com.github.avrokotlin.avro4k.explicitlyNamedSubclassesFrom
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema

@ExperimentalSerializationApi
class PolymorphicClassSchemaFor(private val descriptor: SerialDescriptor,
                                private val namingStrategy: NamingStrategy,
                                private val serializersModule: SerializersModule,
                                private val resolvedSchemas: MutableMap<RecordNaming, Schema>
) : SchemaFor {
   override fun schema(): Schema {
      val subclasses = descriptor.explicitlyNamedSubclassesFrom(serializersModule)
      return Schema.createUnion(
         subclasses.map { ClassSchemaFor(it,namingStrategy,serializersModule, resolvedSchemas).schema() }
      )
   }
}
