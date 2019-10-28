package com.sksamuel.avro4k.encoder

import com.sksamuel.avro4k.Avro
import com.sksamuel.avro4k.ListRecord
import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import kotlinx.serialization.Serializable
import org.apache.avro.util.Utf8

class MapEncoderTest : StringSpec({

   "encode a Map<String, Boolean>" {
      @Serializable
      data class Test(val a: Map<String, Boolean>)

      val schema = Avro.default.schema(Test.serializer())
      val record = Avro.default.toRecord(Test.serializer(), Test(mapOf("a" to true, "b" to false, "c" to true)))
      record shouldBe ListRecord(schema, mapOf(Utf8("a") to true, Utf8("b") to false, Utf8("c") to true))
   }
   "encode a Map<String,String>" {
      @Serializable
      data class Test(val a: Map<String, String>)

      val schema = Avro.default.schema(Test.serializer())
      val record = Avro.default.toRecord(Test.serializer(), Test(mapOf("a" to "x", "b" to "y", "c" to "z")))
      record shouldBe ListRecord(schema, mapOf(Utf8("a") to Utf8("x"), Utf8("b") to Utf8("y"), Utf8("c") to Utf8("z")))
   }
   "encode a Map of records" {

      @Serializable
      data class Foo(val a: String, val b: Boolean)

      @Serializable
      data class Test(val a: Map<String, Foo>)

      val schema = Avro.default.schema(Test.serializer())
      val fooSchema = Avro.default.schema(Foo.serializer())

      val record = Avro.default.toRecord(Test.serializer(), Test(mapOf("a" to Foo("x", true), "b" to Foo("y", false))))
      val xRecord = ListRecord(fooSchema, Utf8("x"), true)
      val yRecord = ListRecord(fooSchema, Utf8("y"), false)

      record shouldBe ListRecord(schema, mapOf(Utf8("a") to xRecord, Utf8("b") to yRecord))
   }
})

