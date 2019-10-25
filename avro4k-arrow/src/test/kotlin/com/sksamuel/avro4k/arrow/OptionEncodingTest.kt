package com.sksamuel.avro4k.arrow

import arrow.core.Option
import arrow.core.some
import com.sksamuel.avro4k.Avro
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import kotlinx.serialization.Serializable

class OptionEncodingTest : FunSpec({

   test("encoding/decoding options") {

      @Serializable
      data class Test(@Serializable(OptionSerializer::class) val a: Option<String>)

      val bytes = Avro.default.dump(Test.serializer(), Test("a".some()))
      Avro.default.load(Test.serializer(), bytes) shouldBe Test("a".some())
   }
})