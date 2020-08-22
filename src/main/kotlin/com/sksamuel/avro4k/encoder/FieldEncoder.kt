package com.sksamuel.avro4k.encoder

import kotlinx.serialization.encoding.Encoder
import org.apache.avro.Schema
import org.apache.avro.generic.GenericFixed
import java.nio.ByteBuffer

interface ExtendedEncoder : Encoder {
   fun encodeByteArray(buffer: ByteBuffer)
   fun encodeFixed(fixed: GenericFixed)
}

interface FieldEncoder : ExtendedEncoder {
   fun addValue(value: Any)
   fun fieldSchema(): Schema
}