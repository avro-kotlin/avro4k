package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.ListRecord
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.StringSpec
import kotlinx.serialization.Serializable
import org.apache.avro.util.Utf8
import java.nio.ByteBuffer

class MapEncoderTest : StringSpec({

   "encode a Map<String, Boolean>" {

      val schema = Avro.default.schema(StringBooleanTest.serializer())
      val record = Avro.default.toRecord(StringBooleanTest.serializer(), StringBooleanTest(mapOf("a" to true, "b" to false, "c" to true)))
      record shouldBe ListRecord(schema, mapOf(Utf8("a") to true, Utf8("b") to false, Utf8("c") to true))
   }

   "encode a Map<String, String>" {

      val schema = Avro.default.schema(StringStringTest.serializer())
      val record = Avro.default.toRecord(StringStringTest.serializer(), StringStringTest(mapOf("a" to "x", "b" to "y", "c" to "z")))
      record shouldBe ListRecord(schema, mapOf(Utf8("a") to Utf8("x"), Utf8("b") to Utf8("y"), Utf8("c") to Utf8("z")))
   }

   "encode a Map<String, ByteArray>" {

      val schema = Avro.default.schema(StringByteArrayTest.serializer())
      val record = Avro.default.toRecord(StringByteArrayTest.serializer(), StringByteArrayTest(mapOf(
         "a" to "x".toByteArray(),
         "b" to "y".toByteArray(),
         "c" to "z".toByteArray()
      )))
      record shouldBe ListRecord(schema, mapOf(
         Utf8("a") to ByteBuffer.wrap("x".toByteArray()),
         Utf8("b") to ByteBuffer.wrap("y".toByteArray()),
         Utf8("c") to ByteBuffer.wrap("z".toByteArray())
      ))
   }

   "encode a Map of records" {

      val schema = Avro.default.schema(StringFooTest.serializer())
      val fooSchema = Avro.default.schema(Foo.serializer())

      val record = Avro.default.toRecord(StringFooTest.serializer(), StringFooTest(mapOf("a" to Foo("x", true), "b" to Foo("y", false))))
      val xRecord = ListRecord(fooSchema, Utf8("x"), true)
      val yRecord = ListRecord(fooSchema, Utf8("y"), false)

      record shouldBe ListRecord(schema, mapOf(Utf8("a") to xRecord, Utf8("b") to yRecord))
   }
}) {
   @Serializable
   data class StringBooleanTest(val a: Map<String, Boolean>)

   @Serializable
   data class StringStringTest(val a: Map<String, String>)

   @Serializable
   data class StringByteArrayTest(val a: Map<String, ByteArray>)

   @Serializable
   data class Foo(val a: String, val b: Boolean)

   @Serializable
   data class StringFooTest(val a: Map<String, Foo>)
}
