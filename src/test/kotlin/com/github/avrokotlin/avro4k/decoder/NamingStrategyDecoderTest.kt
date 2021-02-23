package com.github.avrokotlin.avro4k.decoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroConfiguration
import com.github.avrokotlin.avro4k.schema.PascalCaseNamingStrategy
import com.github.avrokotlin.avro4k.schema.SnakeCaseNamingStrategy
import io.kotest.core.spec.style.WordSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8

class NamingStrategyDecoderTest : WordSpec({
   "Decoder" should {
      @Serializable
      data class Foo(val fooBar: String)

      "support decoding fields with snake_casing" {
         val snakeCaseAvro = Avro.withDefault(AvroConfiguration(SnakeCaseNamingStrategy))
         val record = GenericData.Record(snakeCaseAvro.schema(Foo.serializer())).apply {
            put("foo_bar",Utf8("hello"))
         }

         snakeCaseAvro.fromRecord(Foo.serializer(), record) shouldBe Foo("hello")
      }

      "support decoding fields with PascalCasing" {
         val pascalCaseAvro = Avro.withDefault(AvroConfiguration(PascalCaseNamingStrategy))
         val record = GenericData.Record(pascalCaseAvro.schema(Foo.serializer())).apply {
            put("FooBar",Utf8("hello"))
         }

         pascalCaseAvro.fromRecord(Foo.serializer(), record) shouldBe Foo("hello")
      }
   }
})
