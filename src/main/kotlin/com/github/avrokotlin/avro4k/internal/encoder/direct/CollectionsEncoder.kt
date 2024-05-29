package com.github.avrokotlin.avro4k.internal.encoder.direct

import com.github.avrokotlin.avro4k.Avro
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.AbstractEncoder
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema

internal class MapDirectEncoder(private val schema: Schema, mapSize: Int, avro: Avro, binaryEncoder: org.apache.avro.io.Encoder) :
    AbstractAvroDirectEncoder(avro, binaryEncoder) {
    private var isKey: Boolean = true

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
        isKey = index % 2 == 0
        currentWriterSchema =
            if (isKey) {
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

internal class FixedDirectEncoder(schema: Schema, arraySize: Int, private val avro: Avro, private val binaryEncoder: org.apache.avro.io.Encoder) : AbstractEncoder() {
    private val buffer = ByteArray(schema.fixedSize)
    private var pos = schema.fixedSize - arraySize

    override val serializersModule: SerializersModule
        get() = avro.serializersModule

    init {
        if (arraySize > schema.fixedSize) {
            throw SerializationException("Actual collection size $arraySize is greater than schema fixed size $schema")
        }
    }

    override fun encodeByte(value: Byte) {
        buffer[pos++] = value
    }

    override fun endStructure(descriptor: SerialDescriptor) {
        binaryEncoder.writeFixed(buffer)
    }
}

internal class BytesDirectEncoder(private val avro: Avro, private val binaryEncoder: org.apache.avro.io.Encoder, collectionSize: Int) : AbstractEncoder() {
    private val buffer = ByteArray(collectionSize)
    private var pos = 0

    override val serializersModule: SerializersModule
        get() = avro.serializersModule

    override fun encodeByte(value: Byte) {
        buffer[pos++] = value
    }

    override fun endStructure(descriptor: SerialDescriptor) {
        binaryEncoder.writeBytes(buffer)
    }
}