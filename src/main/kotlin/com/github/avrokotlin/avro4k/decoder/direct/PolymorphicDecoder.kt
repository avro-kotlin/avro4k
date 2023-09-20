package com.github.avrokotlin.avro4k.decoder.direct

import com.github.avrokotlin.avro4k.io.AvroDecoder
import com.github.avrokotlin.avro4k.schema.Resolver
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.modules.SerializersModule

@ExperimentalSerializationApi
class PolymorphicDecoder(
    override val serializersModule: SerializersModule,
    override val decoder: AvroDecoder,
    private val readerUnionAction: Resolver.ReaderUnion
) : StructureDecoder() {
    override var currentAction: Resolver.Action = readerUnionAction
    private enum class State(val elementIndex: Int) {
        KEY(0), VALUE(1), DONE(CompositeDecoder.DECODE_DONE)
    }

    private var decodedState: State? = null

    override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        val nextState = when (decodedState) {
            null -> State.KEY
            State.KEY -> State.VALUE
            State.VALUE -> State.DONE
            State.DONE -> throw IllegalStateException("decodeElementIndex should not be called: value already decoded")
        }
        decodedState = nextState
        return nextState.elementIndex
    }

    override fun decodeString() : String {
        return if(decodedState == State.KEY) decodeSerialName()
        else super.decodeString()
    }
    private fun decodeSerialName(): String {
        val serialName = readerUnionAction.readerSerialNames[readerUnionAction.firstMatch]
        currentAction = readerUnionAction.actualAction
        return serialName
    }
}