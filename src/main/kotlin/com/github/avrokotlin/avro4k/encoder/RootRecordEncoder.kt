package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.AvroInternalConfiguration
import com.github.avrokotlin.avro4k.Record
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.PolymorphicKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.encoding.AbstractEncoder
import kotlinx.serialization.encoding.CompositeEncoder
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema

@ExperimentalSerializationApi
class RootRecordEncoder(
    private val schema: Schema,
    override val serializersModule: SerializersModule,
    private val configuration: AvroInternalConfiguration,
    private val callback: (Record) -> Unit,
) : AbstractEncoder() {
    override fun beginStructure(descriptor: SerialDescriptor): CompositeEncoder {
        return when (descriptor.kind) {
            is StructureKind.CLASS -> RecordEncoder(schema, serializersModule, configuration, callback)
            is PolymorphicKind -> UnionEncoder(schema, serializersModule, configuration, callback)
            else -> throw SerializationException("Unsupported root element passed to root record encoder")
        }
    }

    override fun endStructure(descriptor: SerialDescriptor) {
    }
}