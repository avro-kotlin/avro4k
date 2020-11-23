package com.github.avrokotlin.avro4k.encoder

import com.sksamuel.avro4k.Avro
import com.sksamuel.avro4k.AvroName
import com.sksamuel.avro4k.ListRecord
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.Serializable
import org.apache.avro.util.Utf8

class AvroNameEncoderTest : FunSpec({

   test("encoder should take into account @AvroName on fields") {
      @Serializable
      data class Foo(@AvroName("bar") val foo: String)
      val schema = Avro.default.schema(Foo.serializer())
      Avro.default.toRecord(Foo.serializer(), Foo("hello")) shouldBe ListRecord(schema, listOf(Utf8("hello")))
   }
})