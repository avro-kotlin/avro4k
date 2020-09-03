@file:UseSerializers(BigIntegerSerializer::class)

package com.sksamuel.avro4k.encoder

import com.sksamuel.avro4k.Avro
import com.sksamuel.avro4k.ListRecord
import com.sksamuel.avro4k.serializer.BigIntegerSerializer
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import org.apache.avro.util.Utf8
import java.math.BigInteger

class BigIntegerEncoderTest : FunSpec({

   test("use string for bigint") {

      @Serializable
      data class Test(val b: BigInteger)

      val schema = Avro.default.schema(Test.serializer())

      val test = Test(BigInteger("123123123123213213213123214325365477686789676234"))

      Avro.default.toRecord(Test.serializer(), schema, test) shouldBe ListRecord(schema, Utf8("123123123123213213213123214325365477686789676234"))
   }

   test("encode nullable big ints") {

      @Serializable
      data class Test(val b: BigInteger?)

      val schema = Avro.default.schema(Test.serializer())
      Avro.default.toRecord(Test.serializer(), schema, Test(BigInteger("12312312312321312365477686789676234"))) shouldBe
         ListRecord(schema, Utf8("12312312312321312365477686789676234"))
      Avro.default.toRecord(Test.serializer(), schema, Test(null)) shouldBe ListRecord(schema, null)

   }
})