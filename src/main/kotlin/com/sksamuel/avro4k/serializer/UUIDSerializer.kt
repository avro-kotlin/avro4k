package com.sksamuel.avro4k.serializer

import com.sksamuel.avro4k.schema.AvroDescriptor
import kotlinx.serialization.Decoder
import kotlinx.serialization.Encoder
import kotlinx.serialization.KSerializer
import kotlinx.serialization.PrimitiveKind
import kotlinx.serialization.SerialDescriptor
import kotlinx.serialization.Serializer
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import java.util.*

@Serializer(forClass = UUID::class)
class UUIDSerializer : KSerializer<UUID> {

   override val descriptor: SerialDescriptor = object : AvroDescriptor("uuid", PrimitiveKind.STRING) {
      override fun schema(annos: List<Annotation>): Schema {
         val schema = SchemaBuilder.builder().stringType()
         return LogicalTypes.uuid().addToSchema(schema)
      }
   }

   override fun serialize(encoder: Encoder, obj: UUID) {
      encoder.encodeString(obj.toString())
   }

   override fun deserialize(decoder: Decoder): UUID =
      UUID.fromString(decoder.decodeString())
}