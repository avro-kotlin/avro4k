package com.github.avrokotlin.avro4k.decoder

import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.encoding.Decoder

@ExperimentalSerializationApi
abstract class AvroStructureDecoder : AbstractAvroDecoder(), CompositeDecoder {

    open fun <T : Any?> decodeSerializableValue(
        deserializer: DeserializationStrategy<T>,
        previousValue: T? = null
    ): T = decodeSerializableValue(deserializer)

    final override fun decodeBooleanElement(descriptor: SerialDescriptor, index: Int): Boolean = decodeBoolean()
    final override fun decodeByteElement(descriptor: SerialDescriptor, index: Int): Byte = decodeByte()
    final override fun decodeShortElement(descriptor: SerialDescriptor, index: Int): Short = decodeShort()
    final override fun decodeIntElement(descriptor: SerialDescriptor, index: Int): Int = decodeInt()
    final override fun decodeLongElement(descriptor: SerialDescriptor, index: Int): Long = decodeLong()
    final override fun decodeFloatElement(descriptor: SerialDescriptor, index: Int): Float = decodeFloat()
    final override fun decodeDoubleElement(descriptor: SerialDescriptor, index: Int): Double = decodeDouble()
    final override fun decodeCharElement(descriptor: SerialDescriptor, index: Int): Char = decodeChar()
    final override fun decodeStringElement(descriptor: SerialDescriptor, index: Int): String = decodeString()

    final override fun decodeInlineElement(
        descriptor: SerialDescriptor,
        index: Int
    ): Decoder = decodeInline(descriptor.getElementDescriptor(index))

    final override fun <T> decodeSerializableElement(
        descriptor: SerialDescriptor,
        index: Int,
        deserializer: DeserializationStrategy<T>,
        previousValue: T?
    ): T = decodeSerializableValue(deserializer, previousValue)

    final override fun <T : Any> decodeNullableSerializableElement(
        descriptor: SerialDescriptor,
        index: Int,
        deserializer: DeserializationStrategy<T?>,
        previousValue: T?
    ): T? {
        val isNullabilitySupported = deserializer.descriptor.isNullable
        return if (isNullabilitySupported || decodeNotNullMark()) decodeSerializableValue(deserializer, previousValue) else decodeNull()
    }

    override fun endStructure(descriptor: SerialDescriptor) {
        // nothing by default
    }
}