package com.github.avrokotlin.avro4k.serializer

import com.github.avrokotlin.avro4k.decoder.ExtendedDecoder
import com.github.avrokotlin.avro4k.encoder.ExtendedEncoder
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.Serializer
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.buildSerialDescriptor
import org.apache.avro.Schema
import java.math.BigInteger

@OptIn(ExperimentalSerializationApi::class)
@Serializer(forClass = BigInteger::class)
class BigIntegerSerializer : AvroSerializer<BigInteger>() {

   @OptIn(InternalSerializationApi::class)
   override val descriptor = buildSerialDescriptor(BigInteger::class.qualifiedName!!, PrimitiveKind.STRING)

   override fun encodeAvroValue(schema: Schema, encoder: ExtendedEncoder, obj: BigInteger) =
      encoder.encodeString(obj.toString())

   override fun decodeAvroValue(schema: Schema, decoder: ExtendedDecoder): BigInteger {
      return BigInteger(decoder.decodeString())
   }
}