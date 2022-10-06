package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.Avro
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

class MapSchemaTest : FunSpec({

   test("generate map type for a Map of strings") {

      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/map_string.json"))
      val schema = Avro.default.schema(StringStringTest.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }

   test("generate map type for a Map of strings with value class") {

      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/map_string.json"))
      val schema = Avro.default.schema(WrappedStringStringTest.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }

   test("generate map type for a Map of ints") {

      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/map_int.json"))
      val schema = Avro.default.schema(StringIntTest.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }

   test("generate map type for a Map of records") {

      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/map_record.json"))
      val schema = Avro.default.schema(StringNestedTest.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }

   test("generate map type for map of nullable booleans") {

      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/map_boolean_null.json"))
      val schema = Avro.default.schema(StringBooleanTest.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }

   test("support maps of sets of records") {

      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/map_set_nested.json"))
      val schema = Avro.default.schema(StringSetNestedTest.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }

   test("support array of maps") {

      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/array_of_maps.json"))
      val schema = Avro.default.schema(ArrayTest.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }

   test("support array of maps") {

      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/array_of_maps.json"))
      val schema = Avro.default.schema(WrappedStringArrayTest.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }

   test("support lists of maps") {

      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/list_of_maps.json"))
      val schema = Avro.default.schema(ListTest.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }

   test("support sets of maps") {

      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/set_of_maps.json"))
      val schema = Avro.default.schema(SetTest.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }

   test("support data class of list of data class with maps") {

      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/class_of_list_of_maps.json"))
      val schema = Avro.default.schema(List2Test.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }

}) {
   @Serializable
   @SerialName("mapStringStringTest")
   data class StringStringTest(val map: Map<String, String>)

   @Serializable
   @SerialName("mapStringStringTest")
   data class WrappedStringStringTest(val map: Map<WrappedString, String>)

   @Serializable
   @JvmInline
   value class WrappedString(val value: String)

   @Serializable
   data class StringIntTest(val map: Map<String, Int>)

   @Serializable
   data class Nested(val goo: String)

   @Serializable
   data class StringNestedTest(val map: Map<String, Nested>)

   @Serializable
   data class StringBooleanTest(val map: Map<String, Boolean?>)

   @Serializable
   data class StringSetNestedTest(val map: Map<String, Set<Nested>>)

   @Serializable
   @SerialName("arrayOfMapStringString")
   data class ArrayTest(val array: Array<Map<String, String>>)

   @Serializable
   @SerialName("arrayOfMapStringString")
   data class WrappedStringArrayTest(val array: Array<Map<WrappedString, String>>)

   @Serializable
   data class ListTest(val list: List<Map<String, String>>)

   @Serializable
   data class SetTest(val set: Set<Map<String, String>>)

   @Serializable
   data class Ship(val map: Map<String, String>)

   @Serializable
   data class List2Test(val ship: List<Map<String, String>>)
}
