package com.github.avrokotlin.avro4k.internal.decoder.direct

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.internal.IllegalIndexedAccessError
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor
import org.apache.avro.Schema

internal class ArrayBlockDirectDecoder(
    private val arraySchema: Schema,
    private val decodeFirstBlock: Boolean,
    private val onCollectionSizeDecoded: (Int) -> Unit,
    avro: Avro,
    binaryDecoder: org.apache.avro.io.Decoder,
) : AbstractAvroDirectDecoder(avro, binaryDecoder) {
    override lateinit var currentWriterSchema: Schema

    override fun decodeCollectionSize(descriptor: SerialDescriptor): Int {
        return if (decodeFirstBlock) {
            binaryDecoder.readArrayStart().toInt()
        } else {
            binaryDecoder.arrayNext().toInt()
        }.also { onCollectionSizeDecoded(it) }
    }

    override fun beginElement(
        descriptor: SerialDescriptor,
        index: Int,
    ) {
        // reset the current writer schema in of the element is a union (as a union is resolved)
        currentWriterSchema = arraySchema.elementType
    }

    @OptIn(ExperimentalSerializationApi::class)
    override fun decodeSequentially() = true

    override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        throw IllegalIndexedAccessError()
    }
}

internal class MapBlockDirectDecoder(
    private val mapSchema: Schema,
    private val decodeFirstBlock: Boolean,
    private val onCollectionSizeDecoded: (Int) -> Unit,
    avro: Avro,
    binaryDecoder: org.apache.avro.io.Decoder,
) : AbstractAvroDirectDecoder(avro, binaryDecoder) {
    override lateinit var currentWriterSchema: Schema

    override fun decodeCollectionSize(descriptor: SerialDescriptor): Int {
        return if (decodeFirstBlock) {
            binaryDecoder.readMapStart().toInt()
        } else {
            binaryDecoder.mapNext().toInt()
        }.also { onCollectionSizeDecoded(it) }
    }

    override fun beginElement(
        descriptor: SerialDescriptor,
        index: Int,
    ) {
        // reset the current writer schema in of the element is a union (as a union is resolved)
        currentWriterSchema = if (index % 2 == 0) KEY_SCHEMA else mapSchema.valueType
    }

    @OptIn(ExperimentalSerializationApi::class)
    override fun decodeSequentially() = true

    override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        throw IllegalIndexedAccessError()
    }

    companion object {
        private val KEY_SCHEMA = Schema.create(Schema.Type.STRING)
    }
}