package com.github.avrokotlin.avro4k.io

import com.github.avrokotlin.avro4k.Avro
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.Serializable
import org.apache.avro.util.Utf8

class BasicIoTest : FunSpec() {
   init {
      test("read write out booleans") {
         writeRead(BooleanTest(true), BooleanTest.serializer())
         writeRead(BooleanTest(false), BooleanTest.serializer()) {
            it["z"] shouldBe false
         }
         writeRead(BooleanTest(true), BooleanTest.serializer()) {
            it["z"] shouldBe true
         }
      }

      test("read write out strings") {
         writeRead(StringTest("Hello world"), StringTest.serializer())
         writeRead(StringTest("Hello world"), StringTest.serializer()) {
            it["z"] shouldBe Utf8("Hello world")
         }
      }

      test("read write out longs") {
         writeRead(LongTest(65653L), LongTest.serializer())
         writeRead(LongTest(65653L), LongTest.serializer()) {
            it["z"] shouldBe 65653L
         }
      }

      test("read write out ints") {
         writeRead(IntTest(44), IntTest.serializer())
      }

      test("read write out doubles") {
         writeRead(DoubleTest(3.235), DoubleTest.serializer())
      }

      test("read write out floats") {
         writeRead(FloatTest(3.4F), FloatTest.serializer())
      }
      test("read write out byte arrays") {
         val expected = ByteArrayTest("ABC".toByteArray())
         writeRead(expected,ByteArrayTest.serializer()){
            val deserialized = Avro.default.fromRecord(ByteArrayTest.serializer(),it)
            expected.z shouldBe deserialized.z
         }
      }
   }

   @Serializable
   data class BooleanTest(val z: Boolean)

   @Serializable
   data class StringTest(val z: String)

   @Serializable
   data class FloatTest(val z: Float)

   @Serializable
   data class DoubleTest(val z: Double)

   @Serializable
   data class IntTest(val z: Int)

   @Serializable
   data class LongTest(val z: Long)

   @Suppress("ArrayInDataClass")
   @Serializable
   data class ByteArrayTest(val z : ByteArray)
}
