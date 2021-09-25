package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.ListRecord
import com.github.avrokotlin.avro4k.schema.ValueClassSchemaTest
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.util.Utf8
import java.util.UUID

class ValueClassEncoderTest : StringSpec({

   "encode value class" {

      val id = ValueClassSchemaTest.StringWrapper("100500")
      val uuid = UUID.randomUUID()
      val uuidStr = uuid.toString()
      val uuidW = ValueClassSchemaTest.UuidWrapper(uuid)
      val schema = Avro.default.schema(ValueClassSchemaTest.ContainsInlineTest.serializer())
      Avro.default.toRecord(ValueClassSchemaTest.ContainsInlineTest.serializer(),
         ValueClassSchemaTest.ContainsInlineTest(id, uuidW)) shouldBe ListRecord(schema, Utf8(id.a), Utf8(uuidStr))
   }
})