package com.sksamuel.avro4k.io

import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import kotlinx.serialization.Serializable
import org.apache.avro.util.Utf8

class MapIoTest : StringSpec({

   "read/write primitive maps"{

      @Serializable
      data class Test(val a: Map<String, Boolean>, val b: Map<String, String>, val c: Map<String, Long>)

      writeRead(
         Test(
            mapOf("a" to true, "b" to false),
            mapOf("a" to "x", "b" to "y"),
            mapOf("a" to 123L, "b" to 999L)
         ),
         Test.serializer()
      ) {
         it["a"] shouldBe listOf(Utf8("foo"), Utf8("boo"))
         it["b"] shouldBe listOf(Utf8("goo"), Utf8("moo"))
         it["c"] shouldBe listOf(Utf8("goo"), Utf8("moo"))
      }
   }
})

