@file:Suppress("UNCHECKED_CAST")

package com.github.avrokotlin.avro4k.io

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.kotest.property.Arb
import io.kotest.property.arbitrary.double
import io.kotest.property.arbitrary.int
import io.kotest.property.arbitrary.long
import io.kotest.property.arbitrary.numericDoubles
import io.kotest.property.checkAll
import kotlinx.serialization.Serializable
import org.apache.avro.generic.GenericArray
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8

class CollectionsIoTest : StringSpec({

   "read / write lists of strings" {

      writeRead(StringListsTest(listOf("foo", "boo"), listOf("goo", "moo")), StringListsTest.serializer())
      writeRead(StringListsTest(listOf("foo", "boo"), listOf("goo", "moo")), StringListsTest.serializer()) {
         it["a"] shouldBe listOf(Utf8("foo"), Utf8("boo"))
         it["b"] shouldBe listOf(Utf8("goo"), Utf8("moo"))
      }
   }

   "read / write sets of booleans" {

      writeRead(BooleanSetsTest(setOf(true, false), setOf(false, true)), BooleanSetsTest.serializer())
      writeRead(BooleanSetsTest(setOf(true, false), setOf(false, true)), BooleanSetsTest.serializer()) {
         it["a"] shouldBe listOf(true, false)
         it["b"] shouldBe listOf(false, true)
      }
   }

   "read / write sets of ints" {

      writeRead(S(setOf(1, 3)), S.serializer())
      writeRead(S(setOf(1, 3)), S.serializer()) {
         (it["t"] as GenericData.Array<Int>).toSet() shouldBe setOf(1, 3)
      }
   }

   "read / write arrays of doubles" {

      // there's a bug in avro with Double.POSINF
      checkAll(
         Arb.double(),
         Arb.double(),
         Arb.double()
      ) { a, b, c ->
         val data = DoubleArrayTest(arrayOf(a, b, c))
         val serializer = DoubleArrayTest.serializer()
         val test : (GenericRecord) -> Any = {it["a"] shouldBe listOf(a,b,c)}
         writeReadData(data, serializer, test = test)
         writeReadBinary(data, serializer, test = test)
      }
   }

   "read / write arrays of double in json" {
      //Json does not support -inf/+inf and NaN
      checkAll(
         Arb.numericDoubles(),
         Arb.numericDoubles(),
         Arb.numericDoubles()
      ) { a, b, c ->
         writeReadJson(DoubleArrayTest(arrayOf(a, b, c)), DoubleArrayTest.serializer()) {
            it["a"] shouldBe listOf(a,b,c)
         }
      }
   }

   "read / write lists of long/ints" {

      checkAll(Arb.long(), Arb.long(), Arb.int(), Arb.int()) { a, b, c, d ->
         writeRead(LongListsTest(listOf(a, b), listOf(c, d)), LongListsTest.serializer()) {
            it["a"] shouldBe listOf(a, b)
            it["b"] shouldBe listOf(c, d)
         }
      }
   }

   "read / write lists of records" {

      val hawaiian = Pizza("hawaiian", listOf(Ingredient("ham", 1.5, 5.6), Ingredient("pineapple", 5.2, 0.2)), false, 391)

      writeRead(hawaiian, Pizza.serializer())
      writeRead(hawaiian, Pizza.serializer()) {
         it["name"] shouldBe Utf8("hawaiian")
         it["vegetarian"] shouldBe false
         it["kcals"] shouldBe 391
         (it["ingredients"] as GenericArray<GenericRecord>)[0]["name"] shouldBe Utf8("ham")
         (it["ingredients"] as GenericArray<GenericRecord>)[1]["sugar"] shouldBe 5.2
      }
   }
}) {
   @Serializable
   data class StringListsTest(val a: List<String>, val b: List<String>)

   @Serializable
   data class BooleanSetsTest(val a: Set<Boolean>, val b: Set<Boolean>)

   @Serializable
   data class S(val t: Set<Int>)

   @Serializable
   data class LongListsTest(val a: List<Long>, val b: List<Int>)

   @Serializable
   data class DoubleArrayTest(val a: Array<Double>)

   @Serializable
   data class Ingredient(val name: String, val sugar: Double, val fat: Double)

   @Serializable
   data class Pizza(val name: String, val ingredients: List<Ingredient>, val vegetarian: Boolean, val kcals: Int)
}
