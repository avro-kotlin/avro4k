package com.sksamuel.avro4k

import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import kotlinx.serialization.Serializable

class AvroEncoderTest : FunSpec({

  test("encoding basic data class") {

    @Serializable
    data class Foo(val a: String, val b: Double, val c: Boolean)

   // Avro.default.dump(Foo.serializer(), Foo("hello", 123.456, true)) shouldBe ""
  }

})