package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroConfiguration
import com.github.avrokotlin.avro4k.ListRecord
import com.github.avrokotlin.avro4k.schema.PascalCaseNamingStrategy
import com.github.avrokotlin.avro4k.schema.SnakeCaseNamingStrategy
import io.kotest.core.spec.style.WordSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable
import org.apache.avro.util.Utf8

class NamingStrategyEncoderTest : WordSpec({
   "Encoder" should {
      @Serializable
      data class Foo(val fooBar: String)

      "support encoding fields with snake_casing" {
         val snakeCaseAvro = Avro(AvroConfiguration(SnakeCaseNamingStrategy))

         val schema = snakeCaseAvro.schema(Foo.serializer())

         val record = snakeCaseAvro.toRecord(Foo.serializer(), Foo("hello"))

         record.hasField("foo_bar") shouldBe true

         record shouldBe ListRecord(schema, listOf(Utf8("hello")))
      }

      "support encoding fields with PascalCasing" {
         val pascalCaseAvro = Avro(AvroConfiguration(PascalCaseNamingStrategy))

         val schema = pascalCaseAvro.schema(Foo.serializer())

         val record = pascalCaseAvro.toRecord(Foo.serializer(), Foo("hello"))

         record.hasField("FooBar") shouldBe true

         record shouldBe ListRecord(schema, listOf(Utf8("hello")))
      }
   }
})
