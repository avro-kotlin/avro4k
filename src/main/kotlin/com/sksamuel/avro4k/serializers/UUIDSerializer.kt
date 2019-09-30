package com.sksamuel.avro4k.serializers

import kotlinx.serialization.Decoder
import kotlinx.serialization.Encoder
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerialDescriptor
import kotlinx.serialization.Serializer
import kotlinx.serialization.internal.StringDescriptor
import kotlinx.serialization.withName
import java.util.*

@Serializer(forClass = UUID::class)
class UUIDSerializer : KSerializer<UUID> {

   companion object {
      const val name = "UUID"
   }

   override val descriptor: SerialDescriptor = StringDescriptor.withName(name)

   override fun serialize(encoder: Encoder, obj: UUID) {
      encoder.encodeString(obj.toString())
   }

   override fun deserialize(decoder: Decoder): UUID =
      UUID.fromString(decoder.decodeString())
}