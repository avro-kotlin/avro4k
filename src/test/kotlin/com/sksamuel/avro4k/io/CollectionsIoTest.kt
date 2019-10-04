package com.sksamuel.avro4k.io

import io.kotlintest.properties.Gen
import io.kotlintest.properties.assertAll
import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import kotlinx.serialization.Serializable
import org.apache.avro.generic.GenericArray
import org.apache.avro.generic.GenericRecord
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

   "read / write arrays of doubles" {

      @Serializable
      data class Test(val a: Array<Double>)

      // there's a bug in avro with Double.POSINF
      assertAll(
         Gen.positiveDoubles().filter { it < 1000000 },
         Gen.positiveDoubles().filter { it < 1000000 },
         Gen.positiveDoubles().filter { it < 1000000 }
      ) { a, b, c ->
         writeRead(Test(arrayOf(a, b, c)), Test.serializer()) {
            it["a"] shouldBe listOf(a, b, c)
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

   "read / write lists of records" {

      @Serializable
      data class Ingredient(val name: String, val sugar: Double, val fat: Double)

      @Serializable
      data class Pizza(val name: String, val ingredients: List<Ingredient>, val vegetarian: Boolean, val kcals: Int)

      val hawaiian = Pizza("hawaiian", listOf(Ingredient("ham", 1.5, 5.6), Ingredient("pineapple", 5.2, 0.2)), false, 391)

      writeRead(hawaiian, Pizza.serializer()) {
         it["name"] shouldBe Utf8("hawaiian")
         it["vegetarian"] shouldBe false
         it["kcals"] shouldBe 391
         (it["ingredients"] as GenericArray<GenericRecord>)[0]["name"] shouldBe Utf8("ham")
         (it["ingredients"] as GenericArray<GenericRecord>)[1]["sugar"] shouldBe 5.2
      }
   }
})