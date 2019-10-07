package com.sksamuel.avro4k.serializer

import com.sksamuel.avro4k.decoder.ExtendedDecoder
import com.sksamuel.avro4k.decoder.StringFromAvroValue
import com.sksamuel.avro4k.schema.AvroDescriptor
import kotlinx.serialization.PrimitiveKind
import kotlinx.serialization.SerialDescriptor
import kotlinx.serialization.Serializer
import org.apache.avro.Schema
import org.apache.avro.util.Utf8
import java.math.BigInteger

@Serializer(forClass = BigInteger::class)
class BigIntegerSerializer : AvroSerializer<BigInteger>() {

   override val descriptor: SerialDescriptor = object : AvroDescriptor(BigInteger::class, PrimitiveKind.STRING) {
      override fun schema(annos: List<Annotation>): Schema = Schema.create(Schema.Type.STRING)
   }

   override fun toAvroValue(schema: Schema, value: BigInteger): Any {
      return Utf8(value.toString())
   }

   override fun fromAvroValue(schema: Schema, decoder: ExtendedDecoder): BigInteger {
      return BigInteger(StringFromAvroValue.fromValue(decoder.decodeString()))
   }
}