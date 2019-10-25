package com.sksamuel.avro4k.arrow

import arrow.core.None
import arrow.core.Option
import arrow.core.some
import kotlinx.serialization.Decoder
import kotlinx.serialization.Encoder
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerialDescriptor
import kotlinx.serialization.Serializer
import kotlinx.serialization.internal.nullable

@Serializer(forClass = Option::class)
class OptionSerializer<A : Any>(private val aserializer: KSerializer<A>) : KSerializer<Option<A>> {

   override val descriptor: SerialDescriptor = aserializer.nullable.descriptor

   override fun serialize(encoder: Encoder, obj: Option<A>) {
      return obj.fold(
         { encoder.encodeNull() },
         { aserializer.serialize(encoder, it) }
      )
   }

   final override fun deserialize(decoder: Decoder): Option<A> {
      return if (decoder.decodeNotNullMark()) aserializer.deserialize(decoder).some() else None
   }
}