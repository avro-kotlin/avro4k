package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.Avro
import kotlinx.serialization.descriptors.SerialDescriptor
import org.apache.avro.Schema
import java.nio.ByteBuffer

private val BYTES_SCHEMA = Schema.create(Schema.Type.BYTES)

internal class BytesEncoder(
    override val avro: Avro,
    arraySize: Int,
    private val onEncoded: (ByteBuffer) -> Unit,
) : AvroTaggedEncoder<Int>() {
    private val output: ByteBuffer = ByteBuffer.allocate(arraySize)

    override fun encodeTaggedNull(tag: Int) {
        throw UnsupportedOperationException("nulls are not supported for schema type ${Schema.Type.BYTES}")
    }

    override fun endEncode(descriptor: SerialDescriptor) {
        onEncoded(output.rewind())
    }

    override fun SerialDescriptor.getTag(index: Int) = index

    override val Int.writerSchema: Schema
        get() = BYTES_SCHEMA

    override fun encodeTaggedByte(
        tag: Int,
        value: Byte,
    ) {
        output.put(tag, value)
    }
}