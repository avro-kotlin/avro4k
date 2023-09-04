package com.github.avrokotlin.avro4k.serializer

import com.github.avrokotlin.avro4k.decoder.NativeAvroDecoder
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
       return decodeAvroValue((decoder as NativeAvroDecoder).currentSchema, decoder)
   }

   abstract fun encodeAvroValue(schema: Schema, encoder: NativeAvroEncoder, obj: T)

   abstract fun decodeAvroValue(schema: Schema, decoder: NativeAvroDecoder): T
}
