package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroConfiguration
import com.github.avrokotlin.avro4k.schema.PascalCaseNamingStrategy
import com.github.avrokotlin.avro4k.schema.SnakeCaseNamingStrategy
import io.kotest.core.factory.TestFactory
import io.kotest.core.spec.style.WordSpec
import io.kotest.core.spec.style.wordSpec
import io.kotest.matchers.shouldNotBe
import kotlinx.serialization.Serializable

fun namingStrategyEncoderTests(encoderToTest: EncoderToTest) : TestFactory {
   return wordSpec{
      @Serializable
      data class Foo(val fooBar: String)
      "Encoder" should {
         "support encoding fields with snake_casing" {
            encoderToTest.avro = Avro(AvroConfiguration(SnakeCaseNamingStrategy))
            val schema = encoderToTest.avro.schema(Foo.serializer())
            schema.getField("foo_bar") shouldNotBe null
            encoderToTest.testEncodeDecode(Foo("hello"), record("hello"))
         }

         "support encoding fields with PascalCasing" {
            encoderToTest.avro = Avro(AvroConfiguration(PascalCaseNamingStrategy))
            val schema = encoderToTest.avro.schema(Foo.serializer())
            schema.getField("FooBar") shouldNotBe null
            encoderToTest.testEncodeDecode(Foo("hello"), record("hello"))
         }
      }   
   }
}
class NamingStrategyEncoderTest : WordSpec({
   includeForEveryEncoder { namingStrategyEncoderTests(it) }
})
