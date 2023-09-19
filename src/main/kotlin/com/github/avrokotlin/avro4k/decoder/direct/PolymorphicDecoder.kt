package com.github.avrokotlin.avro4k.decoder.direct

import com.github.avrokotlin.avro4k.io.AvroDecoder
import com.github.avrokotlin.avro4k.schema.Resolver
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.modules.SerializersModule

@ExperimentalSerializationApi
class PolymorphicDecoder(
    override val serializersModule: SerializersModule,
    override val decoder: AvroDecoder,
    override var currentAction: Resolver.Action
) : StructureDecoder() {
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
        val readerUnion = determineReaderUnion()
        currentAction = readerUnion.actualAction
        return readerUnion.readerSerialNames[readerUnion.firstMatch]
    }
    private fun determineReaderUnion() : Resolver.ReaderUnion {
        return when (val resolvedAction = currentAction) {
            is Resolver.ReaderUnion -> resolvedAction
            is Resolver.WriterUnion -> {
                val writerIndex = decoder.readIndex()
                val readerUnion = resolvedAction.actions[writerIndex] as Resolver.ReaderUnion
                readerUnion
            }
            else -> throw SerializationException("No schema defined for resolution.")
        }
    }
}