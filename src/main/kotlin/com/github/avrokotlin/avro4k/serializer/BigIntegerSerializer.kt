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
import org.apache.avro.Schema
import java.math.BigInteger

@OptIn(ExperimentalSerializationApi::class)
@Serializer(forClass = BigInteger::class)
class BigIntegerSerializer : AvroSerializer<BigInteger>() {

   override val descriptor: SerialDescriptor = object : AvroDescriptor(BigInteger::class, PrimitiveKind.STRING) {
      override fun schema(annos: List<Annotation>,
                          serializersModule: SerializersModule,
                          namingStrategy: NamingStrategy): Schema = Schema.create(Schema.Type.STRING)
   }

   override fun encodeAvroValue(schema: Schema, encoder: ExtendedEncoder, obj: BigInteger) =
      encoder.encodeString(obj.toString())

   override fun decodeAvroValue(schema: Schema, decoder: ExtendedDecoder): BigInteger {
      return BigInteger(decoder.decodeString())
   }
}