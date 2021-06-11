package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.ListRecord
import com.github.avrokotlin.avro4k.schema.ValueClassSchemaTest
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.util.Utf8

class ValueClassEncoderTest : StringSpec({

   "encode value class" {

      val id = ValueClassSchemaTest.StringWrapper("100500")
      val schema = Avro.default.schema(ValueClassSchemaTest.ContainsInlineTest.serializer())
      Avro.default.toRecord(ValueClassSchemaTest.ContainsInlineTest.serializer(),
         ValueClassSchemaTest.ContainsInlineTest(id)) shouldBe ListRecord(schema, Utf8(id.a))
   }
})