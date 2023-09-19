package com.github.avrokotlin.avro4k.decoder.direct

import com.github.avrokotlin.avro4k.io.AvroDecoder
import com.github.avrokotlin.avro4k.schema.Resolver
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.modules.SerializersModule

@ExperimentalSerializationApi
class ListDecoder(
    override val decoder: AvroDecoder,
    override val serializersModule: SerializersModule,
    containerAction: Resolver.Container
) : StructureDecoder() {
    override val currentAction: Resolver.Action = containerAction.elementAction
    var index = 0
    var size = 0
    override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        index++
        return if (index == size) CompositeDecoder.DECODE_DONE else index
    }

    override fun decodeCollectionSize(descriptor: SerialDescriptor): Int =
        decoder.readArrayStart().toInt().also { size = it }

    override fun decodeSequentially(): Boolean = true
}