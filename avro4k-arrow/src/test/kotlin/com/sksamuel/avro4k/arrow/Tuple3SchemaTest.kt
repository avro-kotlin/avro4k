package com.sksamuel.avro4k.arrow

import arrow.core.Tuple3
import com.sksamuel.avro4k.Avro
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import kotlinx.serialization.Serializable

class Tuple3SchemaTest : FunSpec({

   test("tuple3 of primitives as record") {

      @Serializable
      data class Test(@Serializable(Tuple3Serializer::class)  val a: Tuple3<String, Boolean, Double>)

      val schema = Avro.default.schema(Test.serializer())
      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/tuple3.json"))
      schema shouldBe expected
   }
})