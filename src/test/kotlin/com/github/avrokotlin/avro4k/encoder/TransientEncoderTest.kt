package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.ListRecord
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.Serializable
import org.apache.avro.util.Utf8

class TransientEncoderTest : FunSpec({

   test("encoder should skip @Transient fields") {

      @Serializable
      data class Test(val a: String, @kotlinx.serialization.Transient val b: String = "foo", val c: String)

      val schema = Avro.default.schema(Test.serializer())
      val record = Avro.default.toRecord(Test.serializer(), Test("a", "b", "c"))
      record shouldBe ListRecord(schema, Utf8("a"), Utf8("c"))
   }
})