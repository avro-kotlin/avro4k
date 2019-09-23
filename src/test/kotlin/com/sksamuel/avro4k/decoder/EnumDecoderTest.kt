package com.sksamuel.avro4k.decoder

import com.sksamuel.avro4k.Avro
import com.sksamuel.avro4k.schema.Wine
import io.kotlintest.shouldBe
import io.kotlintest.specs.WordSpec
import kotlinx.serialization.Serializable
import org.apache.avro.generic.GenericData

@Serializable
data class MyWine(val wine: Wine)

@Serializable
data class NullableWine(val wine: Wine?)

class EnumDecoderTest : WordSpec({

  "Decoder" should {
    "!support enums" {
      val schema = Avro.default.schema(MyWine.serializer())
      val record = GenericData.Record(schema)
      record.put("wine", GenericData.EnumSymbol(schema.getField("wine").schema(), "Malbec"))
      Avro.default.fromRecord(MyWine.serializer(), record) shouldBe MyWine(Wine.Malbec)
    }
    "support nullable enums" {
      val schema = Avro.default.schema(NullableWine.serializer())

      val record1 = GenericData.Record(schema)
      record1.put("wine", GenericData.EnumSymbol(schema.getField("wine").schema(), "Shiraz"))
      Avro.default.fromRecord(NullableWine.serializer(), record1) shouldBe NullableWine(Wine.Shiraz)

      val record2 = GenericData.Record(schema)
      record2.put("wine", null)
      Avro.default.fromRecord(NullableWine.serializer(), record2) shouldBe NullableWine(null)
    }
  }

})