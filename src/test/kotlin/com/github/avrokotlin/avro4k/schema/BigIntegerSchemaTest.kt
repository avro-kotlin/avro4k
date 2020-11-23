@file:UseSerializers(BigIntegerSerializer::class)

package com.github.avrokotlin.avro4k.schema

import com.sksamuel.avro4k.Avro
import com.sksamuel.avro4k.serializer.BigIntegerSerializer
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import java.math.BigInteger

class BigIntegerSchemaTest : FunSpec({

   test("accept big integer as String") {

      @Serializable
      data class Test(val b: BigInteger)

      val schema = Avro.default.schema(Test.serializer())
      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/bigint.json"))
      schema shouldBe expected
   }

   test("accept nullable big integer as String union") {

      @Serializable
      data class Test(val b: BigInteger?)

      val schema = Avro.default.schema(Test.serializer())
      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/bigint_nullable.json"))
      schema shouldBe expected
   }
})