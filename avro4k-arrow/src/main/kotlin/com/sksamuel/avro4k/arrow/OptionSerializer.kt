package com.sksamuel.avro4k.arrow

import arrow.core.None
import arrow.core.Option
import arrow.core.some
import kotlinx.serialization.*
import kotlinx.serialization.builtins.nullable


@Serializer(forClass = Option::class)
class OptionSerializer<A : Any>(private val aserializer: KSerializer<A>) : KSerializer<Option<A>> {

   override val descriptor: SerialDescriptor = aserializer.nullable.descriptor

   override fun serialize(encoder: Encoder, value: Option<A>) {
      return value.fold(
         { encoder.encodeNull() },
         { aserializer.serialize(encoder, it) }
      )
   }

   override fun deserialize(decoder: Decoder): Option<A> {
      return if (decoder.decodeNotNullMark()) aserializer.deserialize(decoder).some() else None
   }
}