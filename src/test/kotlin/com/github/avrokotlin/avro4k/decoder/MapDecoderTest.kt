package com.github.avrokotlin.avro4k.decoder

import com.sksamuel.avro4k.Avro
import com.sksamuel.avro4k.ListRecord
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.StringSpec
import kotlinx.serialization.Serializable
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import java.nio.ByteBuffer

class MapDecoderTest : StringSpec({

   "decode a Map<String, Long> from strings/longs" {
      @Serializable
      data class Test(val a: Map<String, Long>)

      val schema = Avro.default.schema(Test.serializer())

      val record = GenericData.Record(schema)
      record.put("a", mapOf("x" to 152134L, "y" to 917823L))

      Avro.default.fromRecord(Test.serializer(), record) shouldBe Test(mapOf("x" to 152134L, "y" to 917823L))
   }

   "decode a Map<String, Long> from utf8s/longs" {
      @Serializable
      data class Test(val a: Map<String, Long>)

      val schema = Avro.default.schema(Test.serializer())

      val record = GenericData.Record(schema)
      record.put("a", mapOf(Utf8("x") to 152134L, Utf8("y") to 917823L))

      Avro.default.fromRecord(Test.serializer(), record) shouldBe Test(mapOf("x" to 152134L, "y" to 917823L))
   }

   "decode a Map<String, String> from utf8s/utf8s" {
      @Serializable
      data class Test(val a: Map<String, String>)

      val schema = Avro.default.schema(Test.serializer())

      val record = GenericData.Record(schema)
      record.put("a", mapOf(Utf8("x") to Utf8("a"), Utf8("y") to Utf8("b")))

      Avro.default.fromRecord(Test.serializer(), record) shouldBe Test(mapOf("x" to "a", "y" to "b"))
   }

   "decode a Map<String, ByteArray> from utf8s/ByteBuffer" {
      @Serializable
      data class Test(val a: Map<String, ByteArray>) {
         override fun equals(other: Any?): Boolean{
            if (this === other) return true
            if (other?.javaClass != javaClass) return false

            other as Test

            if (a.size != other.a.size) return false
            if (a.keys != other.a.keys) return false

            return a.map {
               it.value.contentEquals(other.a[it.key]!!)
            }.all { it }
         }

         override fun hashCode() = a.hashCode()
      }

      val schema = Avro.default.schema(Test.serializer())

      val record = GenericData.Record(schema)
      record.put("a", mapOf(
         Utf8("a") to ByteBuffer.wrap("x".toByteArray()),
         Utf8("b") to ByteBuffer.wrap("y".toByteArray()),
         Utf8("c") to ByteBuffer.wrap("z".toByteArray())
      ))

      Avro.default.fromRecord(Test.serializer(), record) shouldBe Test(mapOf(
         "a" to "x".toByteArray(),
         "b" to "y".toByteArray(),
         "c" to "z".toByteArray()
      ))
   }

   "decode a Map of records" {

      @Serializable
      data class Foo(val a: String, val b: Boolean)

      @Serializable
      data class Test(val a: Map<String, Foo>)

      val schema = Avro.default.schema(Test.serializer())
      val fooSchema = Avro.default.schema(Foo.serializer())

      val xRecord = ListRecord(fooSchema, Utf8("x"), true)
      val yRecord = ListRecord(fooSchema, Utf8("y"), false)

      val record = GenericData.Record(schema)
      record.put("a", mapOf("a" to xRecord, "b" to yRecord))

      Avro.default.fromRecord(Test.serializer(), record) shouldBe
         Test(mapOf("a" to Foo("x", true), "b" to Foo("y", false)))
   }
})