package com.sksamuel.avro4k.arrow

import arrow.core.Option
import com.sksamuel.avro4k.Avro
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import kotlinx.serialization.Serializable

class OptionSchemaTest : FunSpec({

   test("!options of primitives as basic unions") {

      @Serializable
      data class Test(@Serializable(OptionSerializer::class) val b: Option<String>)

      val schema = Avro.default.schema(Test.serializer())
      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/option.json"))
      schema shouldBe expected
   }
})