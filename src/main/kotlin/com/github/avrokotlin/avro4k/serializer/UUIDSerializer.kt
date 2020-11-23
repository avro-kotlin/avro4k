package com.github.avrokotlin.avro4k.serializer

import com.github.avrokotlin.avro4k.decoder.ExtendedDecoder
import com.github.avrokotlin.avro4k.encoder.ExtendedEncoder
import com.github.avrokotlin.avro4k.schema.AvroDescriptor
import com.github.avrokotlin.avro4k.schema.NamingStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.Serializer
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import java.util.*

@OptIn(ExperimentalSerializationApi::class)
@Serializer(forClass = UUID::class)
class UUIDSerializer : AvroSerializer<UUID>() {

   override fun encodeAvroValue(schema: Schema, encoder: ExtendedEncoder, obj: UUID) = encoder.encodeString(obj.toString())

   override fun decodeAvroValue(schema: Schema, decoder: ExtendedDecoder): UUID =
      UUID.fromString(decoder.decodeString())

   override val descriptor: SerialDescriptor = object : AvroDescriptor("uuid", PrimitiveKind.STRING) {
      override fun schema(annos: List<Annotation>,
                          serializersModule: SerializersModule,
                          namingStrategy: NamingStrategy): Schema {
         val schema = SchemaBuilder.builder().stringType()
         return LogicalTypes.uuid().addToSchema(schema)
      }
   }
}