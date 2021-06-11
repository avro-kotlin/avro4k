@file:UseSerializers(BigDecimalSerializer::class)

package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.ScalePrecision
import com.github.avrokotlin.avro4k.serializer.BigDecimalSerializer
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import java.math.BigDecimal

class BigDecimalSchemaTest : FunSpec({

   test("accept big decimal as logical type on bytes") {

      val schema = Avro.default.schema(BigDecimalTest.serializer())
      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/bigdecimal.json"))
      schema shouldBe expected
   }
   test("accept big decimal as logical type on bytes with custom scale and precision") {

      val schema = Avro.default.schema(BigDecimalPrecisionTest.serializer())
      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/bigdecimal-scale-and-precision.json"))
      schema shouldBe expected
   }
   test("support nullable BigDecimal as a union") {

      val schema = Avro.default.schema(NullableBigDecimalTest.serializer())
      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/bigdecimal_nullable.json"))
      schema shouldBe expected
   }
}) {
   @Serializable
   data class BigDecimalTest(val decimal: BigDecimal)

   @Serializable
   data class BigDecimalPrecisionTest(@ScalePrecision(1, 4) val decimal: BigDecimal)

   @Serializable
   data class NullableBigDecimalTest(val decimal: BigDecimal?)
}
