package com.github.avrokotlin.avro4k.decoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.internal.IllegalIndexedAccessError
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.AbstractDecoder
import kotlinx.serialization.modules.SerializersModule

internal class ByteArrayDecoder(
    private val avro: Avro,
    private val bytes: ByteArray,
) : AbstractDecoder() {
    override val serializersModule: SerializersModule
        get() = avro.serializersModule

    private val iterator = bytes.iterator()

    override fun decodeByte() = iterator.nextByte()

    override fun decodeCollectionSize(descriptor: SerialDescriptor): Int {
        return bytes.size
    }

    override fun decodeSequentially() = true

    override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        throw IllegalIndexedAccessError()
    }
}