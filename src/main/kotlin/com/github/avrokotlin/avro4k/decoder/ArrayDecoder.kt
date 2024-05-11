package com.github.avrokotlin.avro4k.decoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.internal.DecodedNullError
import com.github.avrokotlin.avro4k.internal.IllegalIndexedAccessError
import kotlinx.serialization.descriptors.SerialDescriptor
import org.apache.avro.Schema

internal class ArrayDecoder(
    private val collection: Collection<Any?>,
    private val writerSchema: Schema,
    override val avro: Avro,
) : AbstractAvroDecoder() {
    private val iterator = collection.iterator()

    private var currentItem: Any? = null
    private var decodedNullMark = false

    override val currentWriterSchema: Schema
        get() = writerSchema.elementType

    override fun decodeNotNullMark(): Boolean {
        decodedNullMark = true
        currentItem = iterator.next()
        return currentItem != null
    }

    override fun decodeValue(): Any {
        val value = if (decodedNullMark) currentItem else iterator.next()
        decodedNullMark = false
        return value ?: throw DecodedNullError()
    }

    override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        throw IllegalIndexedAccessError()
    }

    override fun decodeCollectionSize(descriptor: SerialDescriptor) = collection.size

    override fun decodeSequentially() = true
}