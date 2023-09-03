package com.github.avrokotlin.avro4k.encoder

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.CompositeEncoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.modules.SerializersModule


@ExperimentalSerializationApi
abstract class PolymorphicEncoder(
        override val serializersModule: SerializersModule,
) : CompositeEncoder {
    abstract fun encodeSerialName(polymorphicTypeDescriptor: SerialDescriptor, serialName: String)
    abstract fun <T> encodeValue(serializer: SerializationStrategy<T>, value: T)

    override fun <T> encodeSerializableElement(descriptor: SerialDescriptor, index: Int, serializer: SerializationStrategy<T>, value: T) {
        encodeValue(serializer, value)
    }

    override fun encodeStringElement(descriptor: SerialDescriptor, index: Int, value: String) {
        encodeSerialName(descriptor.getElementDescriptor(1), value)
    }

    override fun endStructure(descriptor: SerialDescriptor) {
        // Nothing to do at the end of polymorphic encoding. Still open "in case of"
    }

    //region Unexpected methods
    private fun unexpectedCallError() = SerializationException("Unexpected call for polymorphic descriptor kind")

    final  override fun shouldEncodeElementDefault(descriptor: SerialDescriptor, index: Int): Boolean {
        throw unexpectedCallError()
    }

   final override fun encodeBooleanElement(descriptor: SerialDescriptor, index: Int, value: Boolean) {
        throw unexpectedCallError()
    }

    final   override fun encodeByteElement(descriptor: SerialDescriptor, index: Int, value: Byte) {
        throw unexpectedCallError()
    }

    final   override fun encodeCharElement(descriptor: SerialDescriptor, index: Int, value: Char) {
        throw unexpectedCallError()
    }

    final   override fun encodeDoubleElement(descriptor: SerialDescriptor, index: Int, value: Double) {
        throw unexpectedCallError()
    }

    final  override fun encodeFloatElement(descriptor: SerialDescriptor, index: Int, value: Float) {
        throw unexpectedCallError()
    }

    final  override fun encodeInlineElement(descriptor: SerialDescriptor, index: Int): Encoder {
        throw unexpectedCallError()
    }

    final  override fun encodeIntElement(descriptor: SerialDescriptor, index: Int, value: Int) {
        throw unexpectedCallError()
    }

    final  override fun encodeLongElement(descriptor: SerialDescriptor, index: Int, value: Long) {
        throw unexpectedCallError()
    }

    final  override fun <T : Any> encodeNullableSerializableElement(descriptor: SerialDescriptor, index: Int, serializer: SerializationStrategy<T>, value: T?) {
        throw unexpectedCallError()
    }

    final  override fun encodeShortElement(descriptor: SerialDescriptor, index: Int, value: Short) {
        throw unexpectedCallError()
    }
    //endregion
}
