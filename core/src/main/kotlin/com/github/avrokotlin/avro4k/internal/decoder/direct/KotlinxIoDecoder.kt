package com.github.avrokotlin.avro4k.internal.decoder.direct

import kotlinx.io.InternalIoApi
import kotlinx.io.Source
import kotlinx.io.UnsafeIoApi
import kotlinx.io.readAtMostTo
import kotlinx.io.readByteArray
import kotlinx.io.readDoubleLe
import kotlinx.io.readFloatLe
import kotlinx.io.readString
import kotlinx.io.readTo
import kotlinx.io.unsafe.UnsafeBufferOperations
import kotlinx.io.unsafe.withData
import kotlinx.serialization.SerializationException
import org.apache.avro.SystemLimitException
import org.apache.avro.io.Decoder
import org.apache.avro.util.Utf8
import java.nio.ByteBuffer

internal class KotlinxIoDecoder(
    private val source: Source,
) : Decoder() {
    override fun readNull() {
    }

    override fun readBoolean(): Boolean {
        return source.readByte() == 1.toByte()
    }

    override fun readInt(): Int {
        return source.readVarInt()
    }

    override fun readLong(): Long {
        return source.readVarLong()
    }

    override fun readFloat(): Float {
        return source.readFloatLe()
    }

    override fun readDouble(): Double {
        return source.readDoubleLe()
    }

    override fun readString(old: Utf8?): Utf8 {
        return Utf8(source.readByteArray(SystemLimitException.checkMaxStringLength(readLong())))
    }

    override fun readString(): String {
        return source.readString(readLong())
    }

    override fun skipString() {
        source.skip(readLong())
    }

    @OptIn(InternalIoApi::class, UnsafeIoApi::class)
    override fun readBytes(old: ByteBuffer?): ByteBuffer {
        val length = SystemLimitException.checkMaxBytesLength(readLong())
        source.require(length.toLong())
        if (old != null && length <= old.capacity()) {
            old.clear().limit(length)
            source.readAtMostTo(old)
            return old
        } else {
            UnsafeBufferOperations.forEachSegment(source.buffer) { ctx, segment ->
                if (length >= segment.size) {
                    // the bytes can be split across segments.
                    val buffer = ByteBuffer.allocate(length)
                    source.readAtMostTo(buffer)
                    return buffer
                }
                ctx.withData(segment) { bytes, offset, _ ->
                    // IMPORTANT: don't wrap the buffer when length == segment.size
                    //  as the segment will be recycled, so the backing array content can change
                    source.skip(length.toLong())
                    return ByteBuffer.wrap(bytes, offset, length).asReadOnlyBuffer()
                }
            }
        }
        error("Unreachable")
    }

    override fun skipBytes() {
        source.skip(readLong())
    }

    override fun readFixed(bytes: ByteArray) {
        source.readTo(bytes)
    }

    override fun readFixed(
        bytes: ByteArray,
        start: Int,
        length: Int,
    ) {
        source.readTo(bytes, startIndex = start, endIndex = start + length)
    }

    override fun skipFixed(length: Int) {
        source.skip(length.toLong())
    }

    override fun readEnum(): Int {
        return readInt()
    }

    override fun readArrayStart(): Long {
        return doReadItemCount()
    }

    override fun readMapStart(): Long {
        return doReadItemCount()
    }

    override fun arrayNext(): Long {
        return doReadItemCount()
    }

    override fun mapNext(): Long {
        return doReadItemCount()
    }

    override fun skipArray(): Long {
        return doSkipItems()
    }

    override fun skipMap(): Long {
        return doSkipItems()
    }

    private fun doReadItemCount(): Long {
        var result = readLong()
        if (result < 0L) {
            readLong()
            result = -result
        }
        return result
    }

    private fun doSkipItems(): Long {
        var result = readLong()
        while (result < 0L) {
            source.skip(readLong())
            result = readLong()
        }
        return result
    }

    override fun readIndex(): Int {
        return readInt()
    }
}

private const val MAX_VARINT_INT_BYTES = 5

@OptIn(InternalIoApi::class, UnsafeIoApi::class)
private fun Source.readVarInt(): Int {
    // This ensures having at least one byte available to read
    // There are 3 outcomes:
    // 1. There are already all the bytes of the varint available in the buffer: fast mode
    // 2. There are no bytes available in the buffer, so it triggers a read of a full segment, retrieving much more than 1 byte if more available, finally reading the varint in fast mode
    // 3. There is only one byte available in the buffer, while the varint has 2 bytes across segments, so the read will be done in slow mode
    require(1.toLong())

    // We do not really iterate, we just want to peek the first segment
    UnsafeBufferOperations.forEachSegment(buffer) { ctx, segment ->
        // TODO: start in fast mode, finish in slow mode if the varint is split across segments
        if (segment.size < MAX_VARINT_INT_BYTES) {
            // Slow mode: the int may be split across segments
            return decodeVarInt { readByte() }
        }
        ctx.withData(segment) { buf, offset, _ ->
            // Fast mode: we have all the bytes in the current segment
            var len = 0
            val result = decodeVarInt { buf[offset + len++] }
            skip(len.toLong())
            return result
        }
    }
    error("Unreachable")
}

private const val MAX_VARINT_LONG_BYTES = 10

@OptIn(InternalIoApi::class, UnsafeIoApi::class)
private fun Source.readVarLong(): Long {
    request(1.toLong())
    // We do not really iterate, we just want to peek the first segment
    UnsafeBufferOperations.forEachSegment(buffer) { ctx, segment ->
        if (segment.size < MAX_VARINT_LONG_BYTES) {
            // the long can be split across segments
            return decodeVarLong { readByte() }
        }
        ctx.withData(segment) { bytes, offset, _ ->
            var len = 0
            val result = decodeVarLong { bytes[offset + len++] }
            skip(len.toLong())
            return result
        }
    }
    error("Unreachable")
}

private inline fun decodeVarInt(readByte: () -> Byte): Int {
    var b = readByte().toInt()
    var varint = b and 0x7F
    if (b < 0) {
        b = readByte().toInt()
        varint += ((b and 0x7F) shl 7)
        if (b < 0) {
            b = readByte().toInt()
            varint += ((b and 0x7F) shl 14)
            if (b < 0) {
                b = readByte().toInt()
                varint += ((b and 0x7F) shl 21)
                if (b < 0) {
                    b = readByte().toInt()
                    if (b < 0) {
                        throw SerializationException("Invalid varint encoding: more bytes than expected")
                    }
                    varint += (b shl 28)
                }
            }
        }
    }
    return decodeZigZag(varint)
}

private inline fun decodeVarLong(readByte: () -> Byte): Long {
    var b: Int = readByte().toInt()
    var i = b and 0x7F
    if (b < 0) {
        b = readByte().toInt()
        i += ((b and 0x7F) shl 7)
        if (b < 0) {
            b = readByte().toInt()
            i += ((b and 0x7F) shl 14)
            if (b < 0) {
                b = readByte().toInt()
                i += ((b and 0x7F) shl 21)
                if (b < 0) {
                    return decodeVarLong2(i.toLong(), readByte)
                }
            }
        }
    }
    return decodeZigZag(i).toLong()
}

private inline fun decodeVarLong2(lo: Long, readByte: () -> Byte): Long {
    // then next 28 bits (altogether 8 bytes)
    var lo = lo
    var b: Int = readByte().toInt()
    var i = b and 0x7F
    if (b < 0) {
        i = i and 0x7F
        b = readByte().toInt()
        i += ((b and 0x7F) shl 7)
        if (b < 0) {
            b = readByte().toInt()
            i += ((b and 0x7F) shl 14)
            if (b < 0) {
                b = readByte().toInt()
                i += ((b and 0x7F) shl 21)
                if (b < 0) {
                    // Ok 56-bits gone... still going strong!
                    lo = lo or ((i.toLong()) shl 28)
                    b = readByte().toInt()
                    i = b and 0x7F
                    if (b < 0) {
                        b = readByte().toInt()
                        if (b < 0) {
                            throw SerializationException("Invalid varlong encoding: more bytes than expected")
                        }
                        i = i or (b shl 7)
                    }
                    lo = lo or ((i.toLong()) shl 56)
                    return decodeZigZag(lo)
                }
            }
        }
    }
    lo = lo or ((i.toLong()) shl 28)
    return decodeZigZag(lo)
}

private fun decodeZigZag(varint: Int): Int = (varint ushr 1) xor (-(varint and 1))

private fun decodeZigZag(varint: Long): Long = (varint ushr 1) xor (-(varint and 1))