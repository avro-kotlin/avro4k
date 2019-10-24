package com.sksamuel.avro4k.arrow

import arrow.core.Option
import com.sksamuel.avro4k.decoder.FieldDecoder
import com.sksamuel.avro4k.encoder.FieldEncoder
import com.sksamuel.avro4k.schema.extractNonNull
import kotlinx.serialization.Decoder
import kotlinx.serialization.Encoder
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializer
import org.apache.avro.Schema

@Serializer(forClass = Option::class)
class OptionSerializer : KSerializer<Option<Any>> {

   final override fun serialize(encoder: Encoder, obj: Option<Any>) {
      val schema = (encoder as FieldEncoder).fieldSchema()
      val subschema = when (schema.type) {
         Schema.Type.UNION -> schema.extractNonNull()
         else -> schema
      }
      obj.fold(
         { encoder.encodeNull() },
         {

         }
      )
   }

   final override fun deserialize(decoder: Decoder): Option<Any> {
      val schema = (decoder as FieldDecoder).fieldSchema()
      TODO()
   }
}