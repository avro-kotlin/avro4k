package com.github.avrokotlin.avro4k.encoder.direct

import com.github.avrokotlin.avro4k.encoder.avro.ExtendedEncoder
import com.github.avrokotlin.avro4k.encoder.avro.SchemaBasedEncoder
import com.github.avrokotlin.avro4k.io.AvroEncoder
import org.apache.avro.generic.GenericFixed
import java.nio.ByteBuffer

interface DirectExtendedEncoder : ExtendedEncoder {
   val avroEncoder : AvroEncoder
   override fun encodeFixed(fixed: GenericFixed) {
      avroEncoder.writeFixed(fixed.bytes())
   }

   override fun encodeByteArray(buffer: ByteBuffer) {
      avroEncoder.writeBytes(buffer.array())
   }
}

interface DirectFieldEncoder : DirectExtendedEncoder, SchemaBasedEncoder