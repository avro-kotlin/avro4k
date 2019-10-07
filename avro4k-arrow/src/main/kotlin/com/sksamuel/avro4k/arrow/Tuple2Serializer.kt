package com.sksamuel.avro4k.arrow

import arrow.core.Tuple2
import com.sksamuel.avro4k.decoder.ExtendedDecoder
import com.sksamuel.avro4k.encoder.ExtendedEncoder
import com.sksamuel.avro4k.serializer.AvroSerializer
import kotlinx.serialization.SerialDescriptor
import org.apache.avro.Schema

class Tuple2Serializer : AvroSerializer<Tuple2<*, *>>() {

   override fun encodeAvroValue(schema: Schema, encoder: ExtendedEncoder, obj: Tuple2<*, *>) {
   }

   override fun decodeAvroValue(schema: Schema, decoder: ExtendedDecoder): Tuple2<*, *> {
      TODO()
   }

   override val descriptor: SerialDescriptor
      get() = TODO()
}