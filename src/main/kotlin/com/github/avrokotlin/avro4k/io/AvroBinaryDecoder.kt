package com.github.avrokotlin.avro4k.io

import kotlinx.io.Source
import kotlinx.io.readString
import kotlinx.io.readTo
import java.io.IOException

const val EMPTY_STRING = ""

class AvroBinaryDecoder(val source: Source) : AvroDecoder() {
    override fun readNull() {
        //no op
    }

    override fun readBoolean() = source.readByte().toInt() == 1

    override fun readInt(): Int {
        var b: Int = source.readByte().toInt() and 0xff
        var n = b and 0x7f
        if (b > 0x7f) {
            b = source.readByte().toInt() and 0xff
            n = n xor (b and 0x7f shl 7)
            if (b > 0x7f) {
                b = source.readByte().toInt() and 0xff
                n = n xor (b and 0x7f shl 14)
                if (b > 0x7f) {
                    b = source.readByte().toInt() and 0xff
                    n = n xor (b and 0x7f shl 21)
                    if (b > 0x7f) {
                        b = source.readByte().toInt() and 0xff
                        n = n xor (b and 0x7f shl 28)
                        if (b > 0x7f) {
                            throw NumberFormatException("Invalid int encoding")
                        }
                    }
                }
            }
        }
        return n ushr 1 xor -(n and 1) // back to two's-complement

    }

    override fun readLong(): Long {
        var b: Int = source.readByte().toInt() and 0xff
        var n = b and 0x7f
        val l: Long
        if (b > 0x7f) {
            b = source.readByte().toInt() and 0xff
            n = n xor (b and 0x7f shl 7)
            if (b > 0x7f) {
                b = source.readByte().toInt() and 0xff
                n = n xor (b and 0x7f shl 14)
                if (b > 0x7f) {
                    b = source.readByte().toInt() and 0xff
                    n = n xor (b and 0x7f shl 21)
                    if (b > 0x7f) {
                        // only the low 28 bits can be set, so this won't carry
                        // the sign bit to the long
                        l = innerLongDecode(n.toLong())
                    } else {
                        l = n.toLong()
                    }
                } else {
                    l = n.toLong()
                }
            } else {
                l = n.toLong()
            }
        } else {
            l = n.toLong()
        }
        return l ushr 1 xor -(l and 1L) // back to two's-complement

    }

    @Throws(IOException::class)
    private fun innerLongDecode(first4Bytes: Long): Long {
        var l = first4Bytes
        var b: Int = source.readByte().toInt() and 0xff
        l = l xor (b.toLong() and 0x7fL shl 28)
        if (b > 0x7f) {
            b = source.readByte().toInt() and 0xff
            l = l xor (b.toLong() and 0x7fL shl 35)
            if (b > 0x7f) {
                b = source.readByte().toInt() and 0xff
                l = l xor (b.toLong() and 0x7fL shl 42)
                if (b > 0x7f) {
                    b = source.readByte().toInt() and 0xff
                    l = l xor (b.toLong() and 0x7fL shl 49)
                    if (b > 0x7f) {
                        b = source.readByte().toInt() and 0xff
                        l = l xor (b.toLong() and 0x7fL shl 56)
                        if (b > 0x7f) {
                            b = source.readByte().toInt() and 0xff
                            l = l xor (b.toLong() and 0x7fL shl 63)
                            if (b > 0x7f) {
                                throw NumberFormatException("Invalid long encoding")
                            }
                        }
                    }
                }
            }
        }
        return l
    }

    override fun readFloat(): Float = Float.fromBits(source.readInt())

    override fun readDouble(): Double = Double.fromBits(source.readLong())

    override fun readString(): String {
        val length = readLong()
        if (0L != length) {
            return source.readString(length, Charsets.UTF_8)
        }
        return EMPTY_STRING
    }

    override fun skipString() = doSkip(readLong())

    private fun doSkip(length: Long) {
        if (length != 0L) {
            source.skip(length)
        }
    }

    override fun readBytes(): ByteArray {
        val length = readInt()
        val array = ByteArray(length)
        source.readTo(array,0, length)
        return array
    }

    override fun skipBytes() = doSkip(readInt().toLong())

    override fun readFixed(bytes: ByteArray, start: Int, length: Int) = source.readTo(bytes, start, length)

    override fun skipFixed(length: Int) = doSkip(length.toLong())

    override fun readEnum(): Int = readInt()

    override fun readArrayStart() = doReadItemCount()

    override fun arrayNext() = doReadItemCount()

    override fun skipArray() = doSkipItems()

    override fun readMapStart() = doReadItemCount()

    override fun mapNext() = doReadItemCount()

    override fun skipMap() = doSkipItems()

    override fun readIndex() = readInt()
    private fun doReadItemCount(): Long {
        var result = readLong()
        if (result < 0L) {
            // Consume byte-count if present
            readLong()
            result = -result
        }
        return result
    }
    private fun doSkipItems(): Long {
        var result = readLong()
        while (result < 0L) {
            val length = readLong()
            doSkip(length)
            result = readLong()
        }
        return result
    }
}