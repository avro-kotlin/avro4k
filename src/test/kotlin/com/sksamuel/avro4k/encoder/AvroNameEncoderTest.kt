package com.sksamuel.avro4k.encoder

import com.sksamuel.avro4k.ListRecord
import com.sksamuel.avro4k.Avro
import com.sksamuel.avro4k.AvroName
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import kotlinx.serialization.Serializable
import org.apache.avro.util.Utf8

class AvroNameEncoderTest : FunSpec({

   @Serializable
   data class Foo(@AvroName("bar") val foo: String)

   test("encoder should take into account @AvroName on fields") {
      val schema = Avro.default.schema(Foo.serializer())
      Avro.default.toRecord(Foo.serializer(), Foo("hello")) shouldBe ListRecord(schema, listOf(Utf8("hello")))
   }
})