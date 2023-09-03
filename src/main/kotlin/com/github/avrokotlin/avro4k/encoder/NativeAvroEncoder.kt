package com.github.avrokotlin.avro4k.encoder

import kotlinx.serialization.encoding.Encoder
import org.apache.avro.Schema
import org.apache.avro.generic.GenericFixed
import java.nio.ByteBuffer

interface NativeAvroEncoder : Encoder {
    val currentResolvedSchema: Schema
    fun encodeBytes(value: ByteBuffer)
    fun encodeFixed(value: GenericFixed)
}
