package com.github.avrokotlin.avro4k.decoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.schema.ValueClassSchemaTest.ContainsInlineTest
import com.github.avrokotlin.avro4k.schema.ValueClassSchemaTest.StringWrapper
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.StringSpec
import org.apache.avro.generic.GenericData

class ValueClassDecoderTest : StringSpec({

   "decode value class" {

      val id = StringWrapper("100500")
      val schema = Avro.default.schema(ContainsInlineTest.serializer())
      val record = GenericData.Record(schema)
      record.put("id", id.a)

      Avro.default.fromRecord(ContainsInlineTest.serializer(), record) shouldBe ContainsInlineTest(id)
   }
})