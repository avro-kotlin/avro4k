package com.sksamuel.avro4k.serializer

import com.sksamuel.avro4k.decoder.FieldDecoder
import com.sksamuel.avro4k.encoder.FieldEncoder
import kotlinx.serialization.Decoder
import kotlinx.serialization.Encoder
import kotlinx.serialization.KSerializer
import org.apache.avro.Schema

abstract class AvroSerializer<T> : KSerializer<T> {

   final override fun serialize(encoder: Encoder, obj: T) {
      val schema = (encoder as FieldEncoder).fieldSchema()
      val encoded = toAvroValue(schema, obj)
      encoder.addValue(encoded)
   }

   final override fun deserialize(decoder: Decoder): T {
      val schema = (decoder as FieldDecoder).fieldSchema()
      return fromAvroValue(schema, decoder)
   }

   abstract fun toAvroValue(schema: Schema, value: T): Any
   abstract fun fromAvroValue(schema: Schema, decoder: Decoder): T
}