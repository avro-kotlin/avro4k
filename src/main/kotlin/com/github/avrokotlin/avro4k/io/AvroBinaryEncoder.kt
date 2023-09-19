/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.avrokotlin.avro4k.io

import kotlinx.io.Buffer
import kotlinx.io.Sink
import org.apache.avro.Schema

const val DEFAULT_BUFFER_SIZE = 2048

/**
 * An [AvroEncoder] for Avro's binary encoding.
 *
 * This encoder uses an internal buffer to speed up serialization.
 */
class AvroBinaryEncoder(val sink: Sink, bufferSize: Int = DEFAULT_BUFFER_SIZE) : AvroEncoder() {
    private val buf: ByteArray = ByteArray(bufferSize)
    private var pos = 0
    private var bulkLimit: Int

    init {
        bulkLimit = bufferSize ushr 1
        if (bulkLimit > 512) {
            bulkLimit = 512
        }
    }

    override fun writeNull() {
        //no op
    }

    override fun flush() {
        flushBuffer()
        sink.flush()
    }

    private fun flushBuffer() {
        if (pos > 0) {
            try {
                sink.write(buf, 0, pos)
            } finally {
                pos = 0
            }
        }
    }

    private fun ensureBounds(num: Int) {
        val remaining = buf.size - pos
        if (remaining < num) {
            flushBuffer()
        }
    }

    override fun writeUnionSchema(unionSchema: Schema, indexOfActualSchema: Int) {
        writeInt(indexOfActualSchema)
    }

    override fun writeByte(value: Byte) {
        // inlined, shorter version of ensureBounds
        if (buf.size == pos) {
            flushBuffer()
        }
        buf[pos++] = value
    }

    override fun writeBoolean(b: Boolean) {
        // inlined, shorter version of ensureBounds
        if (buf.size == pos) {
            flushBuffer()
        }
        buf[pos++] = if (b) 1.toByte() else 0.toByte()
    }


    override fun writeInt(n: Int) {
        ensureBounds(5)
        // move sign to low-order bit, and flip others if negative
        var n = n
        n = n shl 1 xor (n shr 31)
        if (n and 0x7F.inv() != 0) {
            buf[pos++] = ((n or 0x80 and 0xFF).toByte())
            n = n ushr 7
            if (n > 0x7F) {
                buf[pos++] = ((n or 0x80 and 0xFF).toByte())
                n = n ushr 7
                if (n > 0x7F) {
                    buf[pos++] = ((n or 0x80 and 0xFF).toByte())
                    n = n ushr 7
                    if (n > 0x7F) {
                        buf[pos++] = ((n or 0x80 and 0xFF).toByte())
                        n = n ushr 7
                    }
                }
            }
        }
        buf[pos++] = (n.toByte())
    }

    override fun writeLong(n: Long) {
        ensureBounds(10)
        // move sign to low-order bit, and flip others if negative
        var n = n
        n = n shl 1 xor (n shr 63)
        if (n and 0x7FL.inv() != 0L) {
            buf[pos++] = ((n or 0x80L and 0xFFL).toByte())
            n = n ushr 7
            if (n > 0x7F) {
                buf[pos++] = ((n or 0x80L and 0xFFL).toByte())
                n = n ushr 7
                if (n > 0x7F) {
                    buf[pos++] = ((n or 0x80L and 0xFFL).toByte())
                    n = n ushr 7
                    if (n > 0x7F) {
                        buf[pos++] = ((n or 0x80L and 0xFFL).toByte())
                        n = n ushr 7
                        if (n > 0x7F) {
                            buf[pos++] = ((n or 0x80L and 0xFFL).toByte())
                            n = n ushr 7
                            if (n > 0x7F) {
                                buf[pos++] = ((n or 0x80L and 0xFFL).toByte())
                                n = n ushr 7
                                if (n > 0x7F) {
                                    buf[pos++] = ((n or 0x80L and 0xFFL).toByte())
                                    n = n ushr 7
                                    if (n > 0x7F) {
                                        buf[pos++] = ((n or 0x80L and 0xFFL).toByte())
                                        n = n ushr 7
                                        if (n > 0x7F) {
                                            buf[pos++] = ((n or 0x80L and 0xFFL).toByte())
                                            n = n ushr 7
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        buf[pos++] = (n.toByte())
    }

    override fun writeFloat(f: Float) {
        ensureBounds(4)
        val bits = f.toBits()
        buf[pos + 3] = (bits ushr 24).toByte()
        buf[pos + 2] = (bits ushr 16).toByte()
        buf[pos + 1] = (bits ushr 8).toByte()
        buf[pos] = bits.toByte()
        pos += 4
    }

    override fun writeDouble(d: Double) {
        ensureBounds(8)
        val bits = d.toBits()
        val first = (bits and 0xFFFFFFFFL).toInt()
        val second = (bits ushr 32 and 0xFFFFFFFFL).toInt()
        // the compiler seems to execute this order the best, likely due to
        // register allocation -- the lifetime of constants is minimized.
        // the compiler seems to execute this order the best, likely due to
        // register allocation -- the lifetime of constants is minimized.
        buf[pos] = first.toByte()
        buf[pos + 4] = second.toByte()
        buf[pos + 5] = (second ushr 8).toByte()
        buf[pos + 1] = (first ushr 8).toByte()
        buf[pos + 2] = (first ushr 16).toByte()
        buf[pos + 6] = (second ushr 16).toByte()
        buf[pos + 7] = (second ushr 24).toByte()
        buf[pos + 3] = (first ushr 24).toByte()
        pos += 8
    }

    override fun writeFixed(bytes: Buffer) {
        // always write directly
        flushBuffer()
        sink.transferFrom(bytes)
    }

    override fun writeFixed(bytes: ByteArray) {
        val len = bytes.size
        if (len > bulkLimit) {
            flushBuffer()
            sink.write(bytes)
            return
        }
        ensureBounds(len)
        System.arraycopy(bytes, 0, buf, pos, len)
        pos += len
    }

    fun writeZero() {
        buf[pos++] = 0
    }

    override fun writeString(str: CharSequence) {
        if (str.isEmpty()) {
            writeZero()
        } else if (str is String) {
            writeBytes(str.encodeToByteArray())
        } else {
            writeString(str.toString())
        }
    }

    override fun writeBytes(bytes: Buffer) {
        if (0L == bytes.size) {
            writeZero()
        } else {
            writeInt(bytes.size.toInt())
            writeFixed(bytes)
        }
    }

    override fun writeBytes(bytes: ByteArray) {
        if (bytes.isEmpty()) {
            writeZero()
        } else {
            writeInt(bytes.size)
            writeFixed(bytes)
        }
    }


    override fun writeEnum(e: Int) {
        writeInt(e)
    }


    private fun startNewEncodedCollection(size: Int) {
        if (size > 0) {
            writeLong(size.toLong())
        }
    }

    override fun startItem() {
        //no op
    }

    override fun writeArrayStart(size: Int) = startNewEncodedCollection(size)

    override fun writeMapStart(size: Int) = startNewEncodedCollection(size)

    override fun writeArrayEnd() = writeCurrentEncodedCollection()
    override fun writeMapEnd() = writeCurrentEncodedCollection()

    private fun writeCurrentEncodedCollection() {
        writeZero()
    }

    override fun writeIndex(unionIndex: Int) {
        writeInt(unionIndex)
    }

}
