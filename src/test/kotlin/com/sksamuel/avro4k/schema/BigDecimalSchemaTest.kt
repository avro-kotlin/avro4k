@file:UseSerializers(BigDecimalSerializer::class)

package com.sksamuel.avro4k.schema

import com.sksamuel.avro4k.Avro
import com.sksamuel.avro4k.ScalePrecision
import com.sksamuel.avro4k.serializer.BigDecimalSerializer
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import java.math.BigDecimal

class BigDecimalSchemaTest : FunSpec({

   test("accept big decimal as logical type on bytes") {

      @Serializable
      data class Test(val decimal: BigDecimal)

      val schema = Avro.default.schema(Test.serializer())
      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/bigdecimal.json"))
      schema shouldBe expected
   }
   test("accept big decimal as logical type on bytes with custom scale and precision") {

      @Serializable
      data class Test(@ScalePrecision(1, 4) val decimal: BigDecimal)

      val schema = Avro.default.schema(Test.serializer())
      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/bigdecimal-scale-and-precision.json"))
      schema shouldBe expected
   }
   test("support nullable BigDecimal as a union") {

      @Serializable
      data class Test(val decimal: BigDecimal?)

      val schema = Avro.default.schema(Test.serializer())
      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/bigdecimal_nullable.json"))
      schema shouldBe expected
   }
})

