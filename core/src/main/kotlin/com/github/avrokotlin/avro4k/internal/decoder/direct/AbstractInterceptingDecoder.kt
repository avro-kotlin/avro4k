package com.github.avrokotlin.avro4k.internal.decoder.direct

import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.encoding.Decoder

internal abstract class AbstractInterceptingDecoder : Decoder, CompositeDecoder {
    protected open fun beginElement(
        descriptor: SerialDescriptor,
        index: Int,
    ) {
    }

    override fun decodeInline(descriptor: SerialDescriptor): Decoder = this

    override fun beginStructure(descriptor: SerialDescriptor): CompositeDecoder = this

    override fun endStructure(descriptor: SerialDescriptor) {
    }

    override fun decodeBooleanElement(
        descriptor: SerialDescriptor,
        index: Int,
    ): Boolean {
        beginElement(descriptor, index)
        return decodeBoolean()
    }

    override fun decodeByteElement(
        descriptor: SerialDescriptor,
        index: Int,
    ): Byte {
        beginElement(descriptor, index)
        return decodeByte()
    }

    override fun decodeShortElement(
        descriptor: SerialDescriptor,
        index: Int,
    ): Short {
        beginElement(descriptor, index)
        return decodeShort()
    }

    override fun decodeIntElement(
        descriptor: SerialDescriptor,
        index: Int,
    ): Int {
        beginElement(descriptor, index)
        return decodeInt()
    }

    override fun decodeLongElement(
        descriptor: SerialDescriptor,
        index: Int,
    ): Long {
        beginElement(descriptor, index)
        return decodeLong()
    }

    override fun decodeFloatElement(
        descriptor: SerialDescriptor,
        index: Int,
    ): Float {
        beginElement(descriptor, index)
        return decodeFloat()
    }

    override fun decodeDoubleElement(
        descriptor: SerialDescriptor,
        index: Int,
    ): Double {
        beginElement(descriptor, index)
        return decodeDouble()
    }

    override fun decodeCharElement(
        descriptor: SerialDescriptor,
        index: Int,
    ): Char {
        beginElement(descriptor, index)
        return decodeChar()
    }

    override fun decodeStringElement(
        descriptor: SerialDescriptor,
        index: Int,
    ): String {
        beginElement(descriptor, index)
        return decodeString()
    }

    override fun decodeInlineElement(
        descriptor: SerialDescriptor,
        index: Int,
    ): Decoder {
        beginElement(descriptor, index)
        return decodeInline(descriptor.getElementDescriptor(index))
    }

    override fun <T> decodeSerializableElement(
        descriptor: SerialDescriptor,
        index: Int,
        deserializer: DeserializationStrategy<T>,
        previousValue: T?,
    ): T {
        beginElement(descriptor, index)
        return decodeSerializableValue(deserializer)
    }

    @OptIn(ExperimentalSerializationApi::class)
    override fun <T : Any> decodeNullableSerializableElement(
        descriptor: SerialDescriptor,
        index: Int,
        deserializer: DeserializationStrategy<T?>,
        previousValue: T?,
    ): T? {
        beginElement(descriptor, index)
        return decodeIfNullable(deserializer) {
            decodeSerializableValue(deserializer)
        }
    }
}

@OptIn(ExperimentalSerializationApi::class)
private inline fun <T : Any> Decoder.decodeIfNullable(
    deserializer: DeserializationStrategy<T?>,
    block: () -> T?,
): T? {
    val isNullabilitySupported = deserializer.descriptor.isNullable
    return if (isNullabilitySupported || decodeNotNullMark()) block() else decodeNull()
}