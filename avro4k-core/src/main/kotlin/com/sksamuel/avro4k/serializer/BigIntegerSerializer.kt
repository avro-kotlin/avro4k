package com.sksamuel.avro4k.serializer

import com.sksamuel.avro4k.decoder.ExtendedDecoder
import com.sksamuel.avro4k.encoder.ExtendedEncoder
import com.sksamuel.avro4k.schema.AvroDescriptor
import kotlinx.serialization.PrimitiveKind
import kotlinx.serialization.SerialDescriptor
import kotlinx.serialization.Serializer
import org.apache.avro.Schema
import java.math.BigInteger

@Serializer(forClass = BigInteger::class)
class BigIntegerSerializer : AvroSerializer<BigInteger>() {

   override val descriptor: SerialDescriptor = object : AvroDescriptor(BigInteger::class, PrimitiveKind.STRING) {
      override fun schema(annos: List<Annotation>): Schema = Schema.create(Schema.Type.STRING)
   }

   override fun encodeAvroValue(schema: Schema, encoder: ExtendedEncoder, obj: BigInteger) =
      encoder.encodeString(obj.toString())

   override fun decodeAvroValue(schema: Schema, decoder: ExtendedDecoder): BigInteger {
      return BigInteger(decoder.decodeString())
   }
}