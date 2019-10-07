package com.sksamuel.avro4k.schema

import com.sksamuel.avro4k.Avro
import com.sksamuel.avro4k.serializer.AvroSerializer
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import kotlinx.serialization.Decoder
import kotlinx.serialization.PrimitiveKind
import kotlinx.serialization.SerialDescriptor
import kotlinx.serialization.Serializable
import kotlinx.serialization.Serializer
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder

class UserDefinedSerializerTest : FunSpec({

   test("schema from user-defined-serializer") {

      @Serializer(forClass = String::class)
      class StringAsFixedSerializer : AvroSerializer<String>() {

         override fun toAvroValue(schema: Schema, value: String): Any {
            TODO()
         }

         override fun fromAvroValue(schema: Schema, decoder: Decoder): String {
            TODO()
         }

         override val descriptor: SerialDescriptor = object : AvroDescriptor("fixed-string", PrimitiveKind.STRING) {
            override fun schema(annos: List<Annotation>): Schema {
               return Schema.createFixed("foo", null, null, 10)
            }
         }
      }

      @Serializable
      data class Test(@Serializable(with = StringAsFixedSerializer::class) val fixed: String)

      Avro.default.schema(Test.serializer()) shouldBe
         SchemaBuilder.record("Test")
            .namespace("com.sksamuel.avro4k.schema.UserDefinedSerializerTest")
            .fields()
            .name("fixed").type(Schema.createFixed("foo", null, null, 10)).noDefault()
            .endRecord()
   }
})