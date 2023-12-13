package com.github.avrokotlin.avro4k.encoder.avro

import kotlinx.serialization.encoding.Encoder
import org.apache.avro.Schema
import org.apache.avro.generic.GenericFixed
import java.nio.ByteBuffer

interface ExtendedEncoder : Encoder {
   fun encodeByteArray(buffer: ByteBuffer)
   fun encodeFixed(fixed: GenericFixed)
}

interface FieldEncoder : ExtendedEncoder, SchemaBasedEncoder {
   fun addValue(value: Any)
}

interface SchemaBasedEncoder {
   fun fieldSchema() : Schema
}