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
    private val containerAction: Resolver.Container
) : StructureDecoder() {
    override var currentAction: Resolver.Action = containerAction.elementAction
    private var index = -1
    private var currentKnownSize = -1
    override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        index++
        if(currentKnownSize == -1) {
            currentKnownSize = decoder.readArrayStart().toInt()
        }else if(index == currentKnownSize) {
            currentKnownSize += decoder.arrayNext().toInt()
        }
        // Always set the action for each container action.
        currentAction = containerAction.elementAction
        return if (index == currentKnownSize) CompositeDecoder.DECODE_DONE else index
    }

    override fun decodeCollectionSize(descriptor: SerialDescriptor): Int = -1
}