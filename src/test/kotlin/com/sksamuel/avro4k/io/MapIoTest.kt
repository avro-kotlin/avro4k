package com.sksamuel.avro4k.io

import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.StringSpec
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
         it["a"] shouldBe mapOf(Utf8("a") to true, Utf8("b") to false)
         it["b"] shouldBe mapOf(Utf8("a") to Utf8("x"), Utf8("b") to Utf8("y"))
         it["c"] shouldBe mapOf(Utf8("a") to 123L, Utf8("b") to 999L)
      }
   }
})

