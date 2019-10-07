package com.sksamuel.avro4k.schema

import io.kotlintest.shouldBe
import io.kotlintest.specs.WordSpec
import kotlinx.serialization.Serializable
import com.sksamuel.avro4k.Avro

class EnumSchemaTest : WordSpec({

  "SchemaEncoder" should {
    "accept enums" {

      @Serializable
      data class EnumTest(val wine: Wine)

      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/enum.json"))
      val schema = Avro.default.schema(EnumTest.serializer())
      schema.toString(true) shouldBe expected.toString(true)
    }
//    "support nullable enum values" {
//
//      @Serializable
//      data class EnumNullable(val maybewine: Wine?)
//
//      val schema = Avro.default.schema(EnumNullable.serializer())
//      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/nullable_enum.json"))
//      schema.toString(true) shouldBe expected.toString(true)
//    }
  }
})

enum class Wine {
  Malbec, Shiraz, CabSav, Merlot
}