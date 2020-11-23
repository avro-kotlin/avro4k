package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroAliases
import com.github.avrokotlin.avro4k.AvroDoc
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.WordSpec
import kotlinx.serialization.Serializable

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
   "Enum with documentation and aliases" should{
      @Serializable
      data class EnumWithDocuTest(
         val value : Suit
      )
      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/enum_with_default.json"))
      val schema = Avro.default.schema(EnumWithDocuTest.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }
})

enum class Wine {
  Malbec, Shiraz, CabSav, Merlot
}
@Serializable
@AvroAliases(["MySuit"])
@AvroDoc("documentation")
enum class Suit{
   SPADES, HEARTS, DIAMONDS, CLUBS;
}