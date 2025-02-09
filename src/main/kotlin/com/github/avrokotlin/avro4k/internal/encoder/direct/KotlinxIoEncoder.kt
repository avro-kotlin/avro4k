package com.github.avrokotlin.avro4k.internal.encoder.direct

import com.github.avrokotlin.avro4k.internal.toIntExact
import kotlinx.io.DelicateIoApi
import kotlinx.io.InternalIoApi
import kotlinx.io.Sink
import kotlinx.io.UnsafeIoApi
import kotlinx.io.unsafe.UnsafeBufferOperations
import kotlinx.io.writeDouble
import kotlinx.io.writeFloat
import kotlinx.io.writeToInternalBuffer
import org.apache.avro.io.BinaryData
import org.apache.avro.io.BinaryEncoder

internal class KotlinxIoEncoder(
    private val sink: Sink,
) : BinaryEncoder() {
    override fun flush() {
        sink.flush()
    }

    override fun writeBoolean(b: Boolean) {
        sink.writeByte(if (b) 1 else 0)
    }

    @OptIn(DelicateIoApi::class, UnsafeIoApi::class)
    override fun writeInt(n: Int) {
        sink.writeToInternalBuffer { buffer ->
            UnsafeBufferOperations.writeToTail(buffer, 5) { bytes, offset, _ ->
                BinaryData.encodeInt(n, bytes, offset)
            }
        }
    }

    @OptIn(DelicateIoApi::class, UnsafeIoApi::class)
    override fun writeLong(n: Long) {
        sink.writeToInternalBuffer { buffer ->
            UnsafeBufferOperations.writeToTail(buffer, 10) { bytes, offset, _ ->
                BinaryData.encodeLong(n, bytes, offset)
            }
        }
    }

    override fun writeFloat(f: Float) {
        sink.writeFloat(f)
    }

    override fun writeDouble(d: Double) {
        sink.writeDouble(d)
    }

    override fun writeFixed(bytes: ByteArray, start: Int, len: Int) {
        sink.write(bytes, start, len)
    }

    override fun writeZero() {
        sink.writeByte(0)
    }

    @OptIn(InternalIoApi::class)
    override fun bytesBuffered(): Int {
        return sink.buffer.size.toIntExact()
    }
}