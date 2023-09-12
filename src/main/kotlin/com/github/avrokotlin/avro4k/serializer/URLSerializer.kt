package com.github.avrokotlin.avro4k.serializer

import com.github.avrokotlin.avro4k.decoder.ExtendedDecoder
import com.github.avrokotlin.avro4k.encoder.ExtendedEncoder
import com.github.avrokotlin.avro4k.schema.AvroDescriptor
import com.github.avrokotlin.avro4k.schema.NamingStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import java.net.URL
import kotlin.reflect.jvm.jvmName


@OptIn(ExperimentalSerializationApi::class)
object URLSerializer : AvroSerializer<URL>() {

   override val descriptor: SerialDescriptor = object : AvroDescriptor(URL::class.jvmName, PrimitiveKind.STRING) {
      override fun schema(annos: List<Annotation>,
                          serializersModule: SerializersModule,
                          namingStrategy: NamingStrategy): Schema = SchemaBuilder.builder().stringType()
   }

   override fun encodeAvroValue(schema: Schema, encoder: ExtendedEncoder, obj: URL) {
      encoder.encodeString(obj.toString())
   }

   override fun decodeAvroValue(schema: Schema, decoder: ExtendedDecoder): URL {
      return when (val v = decoder.decodeAny()) {
         is CharSequence -> URL(v.toString())
         else -> throw SerializationException("Unsupported URL type [$v : ${v?.let { it::class }}]")
      }
   }
}