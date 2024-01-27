package com.github.avrokotlin.avro4k.decoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroAlias
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable
import org.apache.avro.generic.GenericData

@Serializable
data class OldFooString(val s: String)
@Serializable
data class FooStringWithAlias(@AvroAlias("s") val str: String)

class AvroAliasTest : FunSpec({

   test("decode with alias") {
      val schema = Avro.default.schema(OldFooString.serializer())
      val record = GenericData.Record(schema)
      record.put("s", "hello")
      Avro.default.fromRecord(FooStringWithAlias.serializer(), record) shouldBe FooStringWithAlias("hello")
   }
})