package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.AvroName
import io.kotest.core.factory.TestFactory
import io.kotest.core.spec.style.FunSpec
import io.kotest.core.spec.style.wordSpec
import kotlinx.serialization.Serializable

fun avroNameEncodingTests(encoderToTest: EncoderToTest): TestFactory {
   return wordSpec { 
      "encoder" should {
         "take into account @AvroName on fields" {
            @Serializable
            data class Foo(@AvroName("bar") val foo: String)
            encoderToTest.testEncodeDecode(Foo("hello"), record("hello"))
         }
      }
   }
}
class AvroNameEncoderTest : FunSpec({

   includeForEveryEncoder { avroNameEncodingTests(it) }
})
