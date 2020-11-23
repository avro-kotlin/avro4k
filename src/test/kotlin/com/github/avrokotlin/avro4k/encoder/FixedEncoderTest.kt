package com.github.avrokotlin.avro4k.encoder

import com.sksamuel.avro4k.Avro
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.Serializable
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData

class FixedEncoderTest : FunSpec({

   test("encode strings as GenericFixed when schema is Type.FIXED") {

      val schema = SchemaBuilder.record("foo").fields()
         .name("a").type(Schema.createFixed("a", null, null, 5)).noDefault()
         .endRecord()

      @Serializable
      data class Foo(val s: String)

      val record = Avro.default.toRecord(Foo.serializer(), schema, Foo("hello"))
      record[0] shouldBe GenericData.get().createFixed(
         null,
         byteArrayOf(104, 101, 108, 108, 111),
         Schema.createFixed("a", null, null, 5)
      )
   }

   test("encode byte arrays as GenericFixed when schema is Type.FIXED") {

      val schema = SchemaBuilder.record("foo").fields()
         .name("a").type(Schema.createFixed("a", null, null, 5)).noDefault()
         .endRecord()

      @Serializable
      data class Foo(val s: ByteArray)

      val record = Avro.default.toRecord(Foo.serializer(), schema, Foo(byteArrayOf(1, 2, 3, 4, 5)))
      record[0] shouldBe GenericData.get().createFixed(
         null,
         byteArrayOf(1, 2, 3, 4, 5),
         Schema.createFixed("a", null, null, 5)
      )
   }
})