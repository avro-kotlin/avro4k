package com.sksamuel.avro4k.schema

import com.sksamuel.avro4k.Avro
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import kotlinx.serialization.Serializable

@Serializable
data class Foo(val a: String,
               val b: Double,
               val c: Boolean,
               val d: Float,
               val e: Long,
               val f: Int,
               val g: Short,
               val h: Byte)

class BasicSchemaTest : FunSpec({

  test("schema for basic types") {
    Avro.default.schema<Foo>().toString(true).shouldBe("""{
  "type" : "record",
  "name" : "Foo",
  "namespace" : "com.sksamuel.avro4k.schema",
  "fields" : [ {
    "name" : "a",
    "type" : "string"
  }, {
    "name" : "b",
    "type" : "double"
  }, {
    "name" : "c",
    "type" : "boolean"
  }, {
    "name" : "d",
    "type" : "float"
  }, {
    "name" : "e",
    "type" : "long"
  }, {
    "name" : "f",
    "type" : "int"
  }, {
    "name" : "g",
    "type" : "int"
  }, {
    "name" : "h",
    "type" : "int"
  } ]
}""")
  }

})