package com.github.avrokotlin.avro4k.internal.encoder.direct

import com.github.avrokotlin.avro4k.Avro
import kotlinx.serialization.descriptors.SerialDescriptor
import org.apache.avro.Schema

internal class MapDirectEncoder(private val schema: Schema, mapSize: Int, avro: Avro, binaryEncoder: org.apache.avro.io.Encoder) :
    AbstractAvroDirectEncoder(avro, binaryEncoder) {
    init {
        binaryEncoder.writeMapStart()
        binaryEncoder.setItemCount(mapSize.toLong())
    }

    override fun endStructure(descriptor: SerialDescriptor) {
        binaryEncoder.writeMapEnd()
    }

    override lateinit var currentWriterSchema: Schema

    companion object {
        private val STRING_SCHEMA = Schema.create(Schema.Type.STRING)
    }

    override fun encodeElement(
        descriptor: SerialDescriptor,
        index: Int,
    ): Boolean {
        super.encodeElement(descriptor, index)
        currentWriterSchema =
            if (index % 2 == 0) {
                binaryEncoder.startItem()
                STRING_SCHEMA
            } else {
                schema.valueType
            }
        return true
    }
}

internal class ArrayDirectEncoder(
    private val arraySchema: Schema,
    arraySize: Int,
    avro: Avro,
    binaryEncoder: org.apache.avro.io.Encoder,
) : AbstractAvroDirectEncoder(avro, binaryEncoder) {
    init {
        binaryEncoder.writeArrayStart()
        binaryEncoder.setItemCount(arraySize.toLong())
    }

    override lateinit var currentWriterSchema: Schema

    override fun encodeElement(
        descriptor: SerialDescriptor,
        index: Int,
    ): Boolean {
        super.encodeElement(descriptor, index)
        binaryEncoder.startItem()
        currentWriterSchema = arraySchema.elementType
        return true
    }

    override fun endStructure(descriptor: SerialDescriptor) {
        binaryEncoder.writeArrayEnd()
    }
}