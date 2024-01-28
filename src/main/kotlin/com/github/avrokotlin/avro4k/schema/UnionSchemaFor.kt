package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.AvroConfiguration
import com.github.avrokotlin.avro4k.possibleSerializationSubclasses
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema

@ExperimentalSerializationApi
class UnionSchemaFor(
    private val descriptor: SerialDescriptor,
    private val configuration: AvroConfiguration,
    private val serializersModule: SerializersModule,
    private val resolvedSchemas: MutableMap<RecordName, Schema>,
) : SchemaFor {
    override fun schema(): Schema {
        val leafSerialDescriptors =
            descriptor.possibleSerializationSubclasses(serializersModule).sortedBy { it.serialName }
        return Schema.createUnion(
            leafSerialDescriptors.map {
                ClassSchemaFor(it, configuration, serializersModule, resolvedSchemas).schema()
            }
        )
    }
}