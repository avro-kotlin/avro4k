package com.github.avrokotlin.avro4k.encoder

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.CompositeEncoder
import kotlinx.serialization.encoding.Encoder

@ExperimentalSerializationApi
abstract class AvroStructureEncoder : AbstractAvroEncoder(), CompositeEncoder {
    open fun shouldEncodeElement(descriptor: SerialDescriptor, index: Int): Boolean =
            true

    //region composite methods
    final override fun encodeBooleanElement(descriptor: SerialDescriptor, index: Int, value: Boolean) {
        if (shouldEncodeElement(descriptor, index)) {
            resolveElementSchema(descriptor, index, false)
            encodeBoolean(value)
        }
    }

    final override fun encodeByteElement(descriptor: SerialDescriptor, index: Int, value: Byte) {
        if (shouldEncodeElement(descriptor, index)) {
            resolveElementSchema(descriptor, index, false)
            encodeByte(value)
        }
    }

    final override fun encodeShortElement(descriptor: SerialDescriptor, index: Int, value: Short) {
        if (shouldEncodeElement(descriptor, index)) {
            resolveElementSchema(descriptor, index, false)
            encodeShort(value)
        }
    }

    final override fun encodeIntElement(descriptor: SerialDescriptor, index: Int, value: Int) {
        if (shouldEncodeElement(descriptor, index)) {
            resolveElementSchema(descriptor, index, false)
            encodeInt(value)
        }
    }

    final override fun encodeLongElement(descriptor: SerialDescriptor, index: Int, value: Long) {
        if (shouldEncodeElement(descriptor, index)) {
            resolveElementSchema(descriptor, index, false)
            encodeLong(value)
        }
    }

    final override fun encodeFloatElement(descriptor: SerialDescriptor, index: Int, value: Float) {
        if (shouldEncodeElement(descriptor, index)) {
            resolveElementSchema(descriptor, index, false)
            encodeFloat(value)
        }
    }

    final override fun encodeDoubleElement(descriptor: SerialDescriptor, index: Int, value: Double) {
        if (shouldEncodeElement(descriptor, index)) {
            resolveElementSchema(descriptor, index, false)
            encodeDouble(value)
        }
    }

    final override fun encodeCharElement(descriptor: SerialDescriptor, index: Int, value: Char) {
        if (shouldEncodeElement(descriptor, index)) {
            resolveElementSchema(descriptor, index, false)
            encodeChar(value)
        }
    }

    final override fun encodeStringElement(descriptor: SerialDescriptor, index: Int, value: String) {
        if (shouldEncodeElement(descriptor, index)) {
            resolveElementSchema(descriptor, index, false)
            encodeString(value)
        }
    }

    final override fun encodeInlineElement(descriptor: SerialDescriptor, index: Int): Encoder =
            this

    final override fun <T : Any?> encodeSerializableElement(descriptor: SerialDescriptor, index: Int, serializer: SerializationStrategy<T>, value: T) {
        if (shouldEncodeElement(descriptor, index)) {
            resolveElementSchema(descriptor, index, value == null)
            encodeSerializableValue(serializer, value)
        }
    }

    final override fun <T : Any> encodeNullableSerializableElement(descriptor: SerialDescriptor, index: Int, serializer: SerializationStrategy<T>, value: T?) {
        if (shouldEncodeElement(descriptor, index)) {
            resolveElementSchema(descriptor, index, value == null)
            encodeNullableSerializableValue(serializer, value)
        }
    }
    //endregion
}