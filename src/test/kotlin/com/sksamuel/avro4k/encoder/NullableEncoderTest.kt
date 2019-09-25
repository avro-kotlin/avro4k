package com.sksamuel.avro4k.encoder

import com.sksamuel.avro4k.Avro
import com.sksamuel.avro4k.MapRecord
import com.sksamuel.avro4k.decoder.NullableBoolean
import com.sksamuel.avro4k.decoder.NullableString
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import org.apache.avro.util.Utf8

class NullableEncoderTest : FunSpec({

   test("encode nullable strings") {
      val schema = Avro.default.schema(NullableString.serializer())

      Avro.default.toRecord(NullableString.serializer(), NullableString("hello")) shouldBe
         MapRecord(schema, mapOf("s" to Utf8("hello")))

      Avro.default.toRecord(NullableString.serializer(), NullableString(null)) shouldBe
         MapRecord(schema, mapOf("s" to null))
   }

   test("encode nullable booleans") {
      val schema = Avro.default.schema(NullableBoolean.serializer())

      Avro.default.toRecord(NullableBoolean.serializer(), NullableBoolean(true)) shouldBe
         MapRecord(schema, mapOf("b" to true))

      Avro.default.toRecord(NullableBoolean.serializer(), NullableBoolean(false)) shouldBe
         MapRecord(schema, mapOf("b" to false))

      Avro.default.toRecord(NullableBoolean.serializer(), NullableBoolean(null)) shouldBe
         MapRecord(schema, mapOf("b" to null))
   }
})