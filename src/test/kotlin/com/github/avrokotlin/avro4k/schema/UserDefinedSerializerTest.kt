package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.decoder.ExtendedDecoder
import com.github.avrokotlin.avro4k.encoder.ExtendedEncoder
import com.github.avrokotlin.avro4k.serializer.AvroSerializer
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.Serializable
import kotlinx.serialization.Serializer
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder

class UserDefinedSerializerTest : FunSpec({

   test("schema from user-defined-serializer") {

      @Serializer(forClass = String::class)
      @OptIn(ExperimentalSerializationApi::class)
      class StringAsFixedSerializer : AvroSerializer<String>() {

         override fun encodeAvroValue(schema: Schema, encoder: ExtendedEncoder, obj: String) {
            TODO()
         }

         override fun decodeAvroValue(schema: Schema, decoder: ExtendedDecoder): String {
            TODO()
         }

         override val descriptor: SerialDescriptor = object : AvroDescriptor("fixed-string", PrimitiveKind.STRING) {
            override fun schema(annos: List<Annotation>,
                                serializersModule: SerializersModule,
                                namingStrategy: NamingStrategy): Schema {
               return Schema.createFixed("foo", null, null, 10)
            }
         }
      }

      @Serializable
      data class Test(@Serializable(with = StringAsFixedSerializer::class) val fixed: String)

      Avro.default.schema(Test.serializer()) shouldBe
         SchemaBuilder.record("Test")
            .namespace("com.github.avrokotlin.avro4k.schema.UserDefinedSerializerTest")
            .fields()
            .name("fixed").type(Schema.createFixed("foo", null, null, 10)).noDefault()
            .endRecord()
   }
})