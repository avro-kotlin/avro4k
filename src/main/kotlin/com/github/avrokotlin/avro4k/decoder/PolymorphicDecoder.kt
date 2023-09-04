package com.github.avrokotlin.avro4k.decoder

import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.PolymorphicKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.SerialKind
import kotlinx.serialization.descriptors.elementDescriptors
import kotlinx.serialization.descriptors.getContextualDescriptor
import kotlinx.serialization.descriptors.getPolymorphicDescriptors
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.AvroRuntimeException

@ExperimentalSerializationApi
abstract class PolymorphicDecoder : CompositeDecoder {
    abstract fun decodeSerialName(polymorphicDescriptor: SerialDescriptor, possibleSubclasses: Sequence<SerialDescriptor>): String
    abstract fun <T> decodeValue(deserializer: DeserializationStrategy<T>): T

    private enum class State(val elementIndex: Int) {
        KEY(0), VALUE(1), DONE(CompositeDecoder.DECODE_DONE)
    }

    private var decodedState: State? = null

    final override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        val nextState = when (decodedState) {
            null -> State.KEY
            State.KEY -> State.VALUE
            State.VALUE -> State.DONE
            State.DONE -> throw SerializationException("decodeElementIndex should not be called: value already decoded")
        }
        decodedState = nextState
        return nextState.elementIndex
    }

    final override fun decodeStringElement(descriptor: SerialDescriptor, index: Int) =
        decodeSerialName(descriptor, descriptor.possibleSerializationSubclasses(serializersModule))

    final override fun <T> decodeSerializableElement(descriptor: SerialDescriptor, index: Int, deserializer: DeserializationStrategy<T>, previousValue: T?): T =
        decodeValue(deserializer)

    override fun endStructure(descriptor: SerialDescriptor) {
        // Nothing to do by default
    }

    //region other unexpected composite methods
    private fun unexpectedCallError() = SerializationException("Unexpected call for polymorphic descriptor kind")
    final override fun decodeBooleanElement(descriptor: SerialDescriptor, index: Int): Boolean = throw unexpectedCallError()
    final override fun decodeByteElement(descriptor: SerialDescriptor, index: Int): Byte = throw unexpectedCallError()
    final override fun decodeCharElement(descriptor: SerialDescriptor, index: Int): Char = throw unexpectedCallError()
    final override fun decodeDoubleElement(descriptor: SerialDescriptor, index: Int): Double = throw unexpectedCallError()
    final override fun decodeFloatElement(descriptor: SerialDescriptor, index: Int): Float = throw unexpectedCallError()
    final override fun decodeInlineElement(descriptor: SerialDescriptor, index: Int): Decoder = throw unexpectedCallError()
    final override fun decodeIntElement(descriptor: SerialDescriptor, index: Int): Int = throw unexpectedCallError()
    final override fun decodeLongElement(descriptor: SerialDescriptor, index: Int): Long = throw unexpectedCallError()
    final override fun <T : Any> decodeNullableSerializableElement(descriptor: SerialDescriptor, index: Int, deserializer: DeserializationStrategy<T?>, previousValue: T?): T = throw unexpectedCallError()
    final override fun decodeShortElement(descriptor: SerialDescriptor, index: Int): Short = throw unexpectedCallError()
    //endregion
}

@ExperimentalSerializationApi
fun SerialDescriptor.possibleSerializationSubclasses(serializersModule: SerializersModule): Sequence<SerialDescriptor> {
    return when (this.kind) {
        PolymorphicKind.SEALED -> sequence { yieldAll(getElementDescriptor(1).elementDescriptors) }
            .flatMap { it.possibleSerializationSubclasses(serializersModule) }

        PolymorphicKind.OPEN -> sequence { yieldAll(serializersModule.getPolymorphicDescriptors(this@possibleSerializationSubclasses)) }
            .flatMap { it.possibleSerializationSubclasses(serializersModule) }

        SerialKind.CONTEXTUAL -> sequence {
            yield(serializersModule.getContextualDescriptor(this@possibleSerializationSubclasses)
                ?: throw AvroRuntimeException("Missing contextual serializer for descriptor $this of kind CONTEXTUAL."))
        }
            .flatMap { it.possibleSerializationSubclasses(serializersModule) }

        else -> sequenceOf(this)
    }
}
