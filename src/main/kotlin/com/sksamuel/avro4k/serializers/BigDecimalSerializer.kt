package com.sksamuel.avro4k.serializers

import kotlinx.serialization.Decoder
import kotlinx.serialization.Encoder
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerialDescriptor
import kotlinx.serialization.Serializer
import kotlinx.serialization.internal.ShortDescriptor
import kotlinx.serialization.withName
import java.math.BigDecimal

@Serializer(forClass = BigDecimal::class)
class BigDecimalSerializer : KSerializer<BigDecimal> {

  override val descriptor: SerialDescriptor =
      ShortDescriptor.withName("BigDecimal")

  override fun serialize(encoder: Encoder, obj: BigDecimal) {
    encoder.encodeString(obj.toPlainString())
  }

  override fun deserialize(decoder: Decoder): BigDecimal {
    return BigDecimal(decoder.decodeString())
  }
}