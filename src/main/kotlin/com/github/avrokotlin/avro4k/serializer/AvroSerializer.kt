package com.github.avrokotlin.avro4k.serializer

import com.github.avrokotlin.avro4k.decoder.ExtendedDecoder
import com.github.avrokotlin.avro4k.decoder.FieldDecoder
import com.github.avrokotlin.avro4k.encoder.NativeAvroEncoder
import kotlinx.serialization.KSerializer
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import org.apache.avro.Schema

abstract class AvroSerializer<T> : KSerializer<T> {
   final override fun serialize(encoder: Encoder, value: T) {
      encodeAvroValue((encoder as NativeAvroEncoder).currentResolvedSchema, encoder, value)
   }

   final override fun deserialize(decoder: Decoder): T {
      val schema = (decoder as FieldDecoder).fieldSchema()
      return decodeAvroValue(schema, decoder)
   }

   abstract fun encodeAvroValue(schema: Schema, encoder: NativeAvroEncoder, obj: T)

   abstract fun decodeAvroValue(schema: Schema, decoder: ExtendedDecoder): T
}
