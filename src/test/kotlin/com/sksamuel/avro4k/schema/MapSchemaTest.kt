package com.sksamuel.avro4k.schema

import com.sksamuel.avro4k.Avro
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.Serializable

class MapSchemaTest : FunSpec({

  test("generate map type for a Map of strings") {

    @Serializable
    data class Test(val map: Map<String, String>)

    val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/map_string.json"))
    val schema = Avro.default.schema(Test.serializer())
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("generate map type for a Map of ints") {

    @Serializable
    data class Test(val map: Map<String, Int>)

    val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/map_int.json"))
    val schema = Avro.default.schema(Test.serializer())
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("generate map type for a Map of records") {
    @Serializable
    data class Nested(val goo: String)

    @Serializable
    data class Test(val map: Map<String, Nested>)

    val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/map_record.json"))
    val schema = Avro.default.schema(Test.serializer())
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("generate map type for map of nullable booleans") {
    @Serializable
    data class Test(val map: Map<String, Boolean?>)

    val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/map_boolean_null.json"))
    val schema = Avro.default.schema(Test.serializer())
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("support maps of sets of records") {
    @Serializable
    data class Nested(val goo: String)

    @Serializable
    data class Test(val map: Map<String, Set<Nested>>)

    val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/map_set_nested.json"))
    val schema = Avro.default.schema(Test.serializer())
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("support array of maps") {
    @Serializable
    data class Test(val array: Array<Map<String, String>>)

    val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/array_of_maps.json"))
    val schema = Avro.default.schema(Test.serializer())
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("support lists of maps") {
    @Serializable
    data class Test(val list: List<Map<String, String>>)

    val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/list_of_maps.json"))
    val schema = Avro.default.schema(Test.serializer())
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("support sets of maps") {
    @Serializable
    data class Test(val set: Set<Map<String, String>>)

    val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/set_of_maps.json"))
    val schema = Avro.default.schema(Test.serializer())
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("support data class of list of data class with maps") {
    @Serializable
    data class Ship(val map: Map<String, String>)

    @Serializable
    data class Test(val ship: List<Map<String, String>>)

    val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/class_of_list_of_maps.json"))
    val schema = Avro.default.schema(Test.serializer())
    schema.toString(true) shouldBe expected.toString(true)
  }

})