package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroConfiguration
import com.github.avrokotlin.avro4k.ListRecord
import com.github.avrokotlin.avro4k.schema.PascalCaseNamingStrategy
import com.github.avrokotlin.avro4k.schema.SnakeCaseNamingStrategy
import com.github.avrokotlin.avro4k.shouldBeContentOf
import io.kotest.core.spec.style.WordSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8

class NamingStrategyEncoderTest : WordSpec({
   "Encoder" should {
      "support encoding fields with snake_casing" {
         val snakeCaseAvro = Avro(AvroConfiguration(SnakeCaseNamingStrategy))

         val schema = snakeCaseAvro.schema(Foo.serializer())

         val record = snakeCaseAvro.encode(Foo.serializer(), Foo("hello"))

         (record as GenericRecord).hasField("foo_bar") shouldBe true

         record shouldBeContentOf ListRecord(schema, listOf(Utf8("hello")))
      }

      "support encoding fields with PascalCasing" {
         val pascalCaseAvro = Avro(AvroConfiguration(PascalCaseNamingStrategy))

         val schema = pascalCaseAvro.schema(Foo.serializer())

         val record = pascalCaseAvro.encode(Foo.serializer(), Foo("hello"))

         (record as GenericRecord).hasField("FooBar") shouldBe true

         record shouldBeContentOf ListRecord(schema, listOf(Utf8("hello")))
      }
   }
}) {
   @Serializable
   data class Foo(val fooBar: String)
}
