package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.ListRecord
import com.github.avrokotlin.avro4k.shouldBeContentOf
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.Serializable
import org.apache.avro.util.Utf8

class TransientEncoderTest : FunSpec({

   test("encoder should skip @Transient fields") {

      val schema = Avro.default.schema(Test.serializer())
      val record = Avro.default.encode(Test.serializer(), Test("a", "b", "c"))
      record shouldBeContentOf ListRecord(schema, Utf8("a"), Utf8("c"))
   }
}) {
   @Serializable
   data class Test(val a: String, @kotlinx.serialization.Transient val b: String = "foo", val c: String)
}
