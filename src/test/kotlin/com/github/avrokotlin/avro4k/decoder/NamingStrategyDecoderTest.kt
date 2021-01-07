package com.github.avrokotlin.avro4k.decoder

import com.github.avrokotlin.avro4k.Avro
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
         val record = GenericData.Record(Avro.default.schema(Foo.serializer(), SnakeCaseNamingStrategy)).apply {
            put("foo_bar",Utf8("hello"))
         }

         Avro.default.fromRecord(Foo.serializer(), record, SnakeCaseNamingStrategy) shouldBe Foo("hello")
      }

      "support decoding fields with PascalCasing" {
         val record = GenericData.Record(Avro.default.schema(Foo.serializer(), PascalCaseNamingStrategy)).apply {
            put("FooBar",Utf8("hello"))
         }

         Avro.default.fromRecord(Foo.serializer(), record, PascalCaseNamingStrategy) shouldBe Foo("hello")
      }
   }
})
