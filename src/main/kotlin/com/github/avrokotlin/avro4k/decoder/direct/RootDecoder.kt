package com.github.avrokotlin.avro4k.decoder.direct

import com.github.avrokotlin.avro4k.io.AvroDecoder
import com.github.avrokotlin.avro4k.schema.Resolver
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.modules.SerializersModule

@OptIn(ExperimentalSerializationApi::class)
class RootDecoder(
    override val serializersModule: SerializersModule,
    override val decoder: AvroDecoder,
    override var currentAction: Resolver.Action
) : StructureDecoder() {
    var decoded = false
    override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        val index = if(decoded) CompositeDecoder.DECODE_DONE else 0
        decoded = true
        return index
    }

    override fun beginStructure(descriptor: SerialDescriptor): CompositeDecoder {
        return super.beginStructure(descriptor)
    }
}