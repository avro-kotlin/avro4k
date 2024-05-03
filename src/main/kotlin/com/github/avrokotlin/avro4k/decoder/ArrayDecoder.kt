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
) : AvroTaggedDecoder<Schema>() {
    private val iterator = collection.iterator()
    private val elementType = if (writerSchema.type == Schema.Type.BYTES) writerSchema else writerSchema.elementType

    private var currentItem: Any? = null
    private var decodedNullMark = false

    override val Schema.writerSchema: Schema
        get() = this@ArrayDecoder.elementType

    override fun SerialDescriptor.getTag(index: Int): Schema {
        return elementType
    }

    override fun decodeTaggedNotNullMark(tag: Schema): Boolean {
        decodedNullMark = true
        currentItem = iterator.next()
        return currentItem != null
    }

    override fun decodeTaggedValue(tag: Schema): Any {
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