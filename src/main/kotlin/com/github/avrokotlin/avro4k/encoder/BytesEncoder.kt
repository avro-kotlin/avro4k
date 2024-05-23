package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.Avro
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.AbstractEncoder
import kotlinx.serialization.modules.SerializersModule
import java.nio.ByteBuffer

internal class BytesEncoder(
    private val avro: Avro,
    arraySize: Int,
    private val onEncoded: (ByteBuffer) -> Unit,
) : AbstractEncoder() {
    private val output: ByteBuffer = ByteBuffer.allocate(arraySize)

    override val serializersModule: SerializersModule
        get() = avro.serializersModule

    override fun endStructure(descriptor: SerialDescriptor) {
        onEncoded(output.rewind())
    }

    override fun encodeByte(value: Byte) {
        output.put(value)
    }
}