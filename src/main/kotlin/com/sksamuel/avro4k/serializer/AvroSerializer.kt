package com.sksamuel.avro4k.serializer

import com.sksamuel.avro4k.decoder.ExtendedDecoder
import com.sksamuel.avro4k.decoder.FieldDecoder
import com.sksamuel.avro4k.encoder.ExtendedEncoder
import com.sksamuel.avro4k.encoder.FieldEncoder
import com.sksamuel.avro4k.schema.extractNonNull
import kotlinx.serialization.Decoder
import kotlinx.serialization.Encoder
import kotlinx.serialization.KSerializer
import org.apache.avro.Schema


abstract class AvroSerializer<T> : KSerializer<T> {

   final override fun serialize(encoder: Encoder, obj: T) {
      val schema = (encoder as FieldEncoder).fieldSchema()
      // we may be encoding a nullable schema
      val subschema = when (schema.type) {
         Schema.Type.UNION -> schema.extractNonNull()
         else -> schema
      }
      encodeAvroValue(subschema, encoder, obj)
   }

   final override fun deserialize(decoder: Decoder): T {
      val schema = (decoder as FieldDecoder).fieldSchema()
//      // we may be coming from a nullable schema aka a union
//      val subschema = when (schema.type) {
//         Schema.Type.UNION -> schema.extractNonNull()
//         else -> schema
//      }
      return fromAvroValue(schema, decoder)
   }

   abstract fun encodeAvroValue(schema: Schema, encoder: ExtendedEncoder, obj: T)

   abstract fun fromAvroValue(schema: Schema, decoder: ExtendedDecoder): T
}

