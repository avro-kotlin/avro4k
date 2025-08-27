package com.github.avrokotlin.avro4k.internal.decoder.generic

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.internal.DecodedNullError
import com.github.avrokotlin.avro4k.internal.IllegalIndexedAccessError
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor
import org.apache.avro.Schema

internal class MapGenericDecoder(
    private val map: Map<CharSequence, Any?>,
    private val writerSchema: Schema,
    override val avro: Avro,
) : AbstractAvroGenericDecoder() {
    private val iterator = map.asSequence().flatMap { sequenceOf(true to it.key, true to it.value) }.iterator()
    private lateinit var currentData: Pair<Boolean, Any?>
    private var decodedNotNullMark = false

    override val currentWriterSchema: Schema
        get() =
            if (currentData.first) {
                STRING_SCHEMA
            } else {
                writerSchema.valueType
            }

    override fun decodeNotNullMark(): Boolean {
        decodedNotNullMark = true
        currentData = iterator.next()
        return currentData.second != null
    }

    override fun decodeValue(): Any {
        if (!decodedNotNullMark) {
            currentData = iterator.next()
        } else {
            decodedNotNullMark = false
        }
        return currentData.second ?: throw DecodedNullError()
    }

    @OptIn(ExperimentalSerializationApi::class)
    override fun decodeNull(): Nothing? {
        decodedNotNullMark = false
        return null
    }

    override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        throw IllegalIndexedAccessError()
    }

    override fun decodeCollectionSize(descriptor: SerialDescriptor) = map.size

    @OptIn(ExperimentalSerializationApi::class)
    override fun decodeSequentially() = true

    companion object {
        private val STRING_SCHEMA = Schema.create(Schema.Type.STRING)
    }
}

internal class ArrayGenericDecoder(
    private val collection: Collection<Any?>,
    private val writerSchema: Schema,
    override val avro: Avro,
) : AbstractAvroGenericDecoder() {
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

    @OptIn(ExperimentalSerializationApi::class)
    override fun decodeSequentially() = true
}