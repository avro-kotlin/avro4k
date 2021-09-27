package com.github.avrokotlin.avro4k.decoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.schema.ValueClassSchemaTest
import com.github.avrokotlin.avro4k.schema.ValueClassSchemaTest.ContainsInlineTest
import com.github.avrokotlin.avro4k.schema.ValueClassSchemaTest.StringWrapper
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.generic.GenericData
import java.util.UUID

class ValueClassDecoderTest : StringSpec({

   "decode value class" {

      val id = StringWrapper("100500")
      val uuid = ValueClassSchemaTest.UuidWrapper(UUID.randomUUID())
      val schema = Avro.default.schema(ContainsInlineTest.serializer())
      val record = GenericData.Record(schema)
      record.put("id", id.a)
      record.put("uuid", uuid.uuid.toString())

      Avro.default.fromRecord(ContainsInlineTest.serializer(), record) shouldBe ContainsInlineTest(id, uuid)
   }
})