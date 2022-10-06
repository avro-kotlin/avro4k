package com.github.avrokotlin.avro4k

import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.Serializable

class RecordEncoderTest : FunSpec({
   test("encoding basic data class") {
      val input = Foo(
         "string value",
         2.2,
         true,
         S(setOf(1, 2, 3)),
         ValueClass(ByteArray(3) { it.toByte() }) // 0,1,2 array
      )
      val record = Avro.default.toRecord(
         Foo.serializer(), input
      )
      val output = Avro.default.fromRecord(Foo.serializer(), record)
      output shouldBe input
   }
}) {
   @Serializable
   data class Foo(val a: String, val b: Double, val c: Boolean, val s: S, val vc: ValueClass) {
      override fun equals(other: Any?): Boolean {
         if (this === other) return true
         if (other !is Foo) return false

         if (a != other.a) return false
         if (b != other.b) return false
         if (c != other.c) return false
         if (s != other.s) return false
         if (!vc.value.contentEquals(other.vc.value)) return false

         return true
      }

      override fun hashCode(): Int {
         var result = a.hashCode()
         result = 31 * result + b.hashCode()
         result = 31 * result + c.hashCode()
         result = 31 * result + s.hashCode()
         result = 31 * result + vc.value.contentHashCode()
         return result
      }
   }

   @Serializable
   data class S(val t: Set<Int>)

   @JvmInline
   @Serializable
   value class ValueClass(val value: ByteArray)
}
