package com.github.avrokotlin.avro4k.encoder.direct

import com.github.avrokotlin.avro4k.io.AvroEncoder
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema

@ExperimentalSerializationApi
class ListEncoder(
    private val schema: Schema,
    override val serializersModule: SerializersModule,
    override val avroEncoder: AvroEncoder,
    size: Int
) : StructureEncoder() {
    override fun fieldSchema(): Schema = schema.elementType

    init {
        avroEncoder.writeArrayStart(size)
    }

    override fun endStructure(descriptor: SerialDescriptor) {
        avroEncoder.writeArrayEnd()
    }

    override fun <T> encodeSerializableValue(serializer: SerializationStrategy<T>, value: T) {
        avroEncoder.startItem()
        super.encodeSerializableValue(serializer, value)
    }
}