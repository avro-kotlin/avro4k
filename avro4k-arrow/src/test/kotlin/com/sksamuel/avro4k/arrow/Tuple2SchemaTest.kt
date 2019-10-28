package com.sksamuel.avro4k.arrow

import arrow.core.Tuple2
import com.sksamuel.avro4k.Avro
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import kotlinx.serialization.Serializable

class Tuple2SchemaTest : FunSpec({

   test("tuples of primitives as basic unions") {

      @Serializable
      data class Test(@Serializable(Tuple2Serializer::class)  val a: Tuple2<String, Boolean>)

      val schema = Avro.default.schema(Test.serializer())
      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/tuple2.json"))
      schema shouldBe expected
   }
})