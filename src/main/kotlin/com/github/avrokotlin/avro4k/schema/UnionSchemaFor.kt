package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.RecordNaming
import com.github.avrokotlin.avro4k.decoder.possibleSerializationSubclasses
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor
import org.apache.avro.Schema

@ExperimentalSerializationApi
class UnionSchemaFor(
    private val descriptor: SerialDescriptor,
    private val avro: Avro,
    private val resolvedSchemas: MutableMap<RecordNaming, Schema>
) : SchemaFor {
    override fun schema(): Schema {        
        val leafSerialDescriptors =
            descriptor.possibleSerializationSubclasses(avro.serializersModule).sortedBy { it.serialName }
        return Schema.createUnion(
            leafSerialDescriptors.map {
                ClassSchemaFor(it, avro, resolvedSchemas).schema()
            }.toList()
        )
    }
}
