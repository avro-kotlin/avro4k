package com.sksamuel.avro4k.io

import io.kotlintest.properties.Gen
import io.kotlintest.properties.assertAll
import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import kotlinx.serialization.Serializable
import org.apache.avro.util.Utf8

class CollectionsIoTest : StringSpec({

   "read / write lists of strings" {

      @Serializable
      data class Test(val a: List<String>, val b: List<String>)

      writeRead(Test(listOf("foo", "boo"), listOf("goo", "moo")), Test.serializer())
      writeRead(Test(listOf("foo", "boo"), listOf("goo", "moo")), Test.serializer()) {
         it["a"] shouldBe listOf(Utf8("foo"), Utf8("boo"))
         it["b"] shouldBe listOf(Utf8("goo"), Utf8("moo"))
      }
   }

   "read / write sets of booleans" {

      @Serializable
      data class Test(val a: Set<Boolean>, val b: Set<Boolean>)

      writeRead(Test(setOf(true, false), setOf(false, true)), Test.serializer())
      writeRead(Test(setOf(true, false), setOf(false, true)), Test.serializer()) {
         it["a"] shouldBe listOf(true, false)
         it["b"] shouldBe listOf(false, true)
      }
   }

   "read / write arrays of floating points" {

      @Serializable
      data class Test(val a: Array<Double>, val b: Array<Float>)

      assertAll(Gen.double(), Gen.double(), Gen.float(), Gen.float()) { a, b, c, d ->
         writeRead(Test(arrayOf(a, b), arrayOf(c, d)), Test.serializer()) {
            it["a"] shouldBe listOf(a, b)
            it["b"] shouldBe listOf(c, d)
         }
      }
   }

   "read / write lists of long/ints" {

      @Serializable
      data class Test(val a: List<Long>, val b: List<Int>)

      assertAll(Gen.long(), Gen.long(), Gen.int(), Gen.int()) { a, b, c, d ->
         writeRead(Test(listOf(a, b), listOf(c, d)), Test.serializer()) {
            it["a"] shouldBe listOf(a, b)
            it["b"] shouldBe listOf(c, d)
         }
      }
   }
})