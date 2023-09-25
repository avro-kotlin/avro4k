package com.github.avrokotlin.avro4k.decoder.direct

import com.github.avrokotlin.avro4k.io.AvroDecoder
import com.github.avrokotlin.avro4k.schema.Resolver
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.modules.SerializersModule

@ExperimentalSerializationApi
class MapDecoder(
    override val decoder: AvroDecoder,
    override val serializersModule: SerializersModule,
    private val containerAction: Resolver.Container
) : StructureDecoder() {
    private var index = -1
    private var currentKnownSize = -1
    override var currentAction: Resolver.Action = containerAction.elementAction
    override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        index++
        if (currentKnownSize == -1) {
            currentKnownSize = decoder.readMapStart().toInt() * 2
        } else if (currentKnownSize == index) {
            currentKnownSize += decoder.mapNext().toInt() * 2
        }
        //Reset the current action for each element.
        currentAction = containerAction.elementAction
        
        return if (index == currentKnownSize) CompositeDecoder.DECODE_DONE else index
    }

    override fun decodeString(): String {
        return if (index % 2 == 0) {
            //Decode a map key. Always encoded as string
            decoder.readString()
        } else {
            super.decodeString()
        }
    }

    override fun decodeCollectionSize(descriptor: SerialDescriptor): Int = -1
}