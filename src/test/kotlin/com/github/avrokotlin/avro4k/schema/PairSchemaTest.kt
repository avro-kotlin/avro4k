package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.Avro
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.Serializable

class PairSchemaTest : FunSpec({

   test("!generate union:T,U for Pair[T,U] of primitives") {

      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/pair.json"))
      val schema = Avro.default.schema(StringDoubleTest.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }
   test("!generate union:T,U for Either[T,U] of records") {

      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/pair_records.json"))
      val schema = Avro.default.schema(GooFooTest.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }
//    "generate union:T,U for Either[T,U] of records using @AvroNamespace" in {
//      @AvroNamespace("mm")
//      data class Goo(s: String)
//      @AvroNamespace("nn")
//      data class Foo(b: Boolean)
//       data class Test(either: Either[Goo, Foo])
//      val expected = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/either_record_with_avro_namespace.json"))
//      val schema = AvroSchema[Test]
//      schema.toString(true) shouldBe expected.toString(true)
//    }
//   test("flatten nested unions and move null to first position") {
//      AvroSchema[Either[String, Option[Int]]].toString shouldBe """["null","string","int"]"""
//   }

}) {
   @Serializable
   data class StringDoubleTest(val p: Pair<String, Double>)

   @Serializable
   data class Goo(val s: String)

   @Serializable
   data class Foo(val b: Boolean)

   @Serializable
   data class GooFooTest(val p: Pair<Goo, Foo>)
}
