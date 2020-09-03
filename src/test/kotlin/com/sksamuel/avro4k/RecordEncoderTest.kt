package com.sksamuel.avro4k

import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.Serializable

class RecordEncoderTest : FunSpec({

   test("encoding basic data class") {

      @Serializable
      data class Foo(val a: String, val b: Double, val c: Boolean)

      // Avro.default.dump(Foo.serializer(), Foo("hello", 123.456, true)) shouldBe ""
   }

   test("to/from records of sets of ints") {

      @Serializable
      data class S(val t: Set<Int>)

      val r = Avro.default.toRecord(S.serializer(), S(setOf(1)))
      val s = Avro.default.fromRecord(S.serializer(), r)  // this line fails
      s.t shouldBe setOf(1)
   }
})