package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.*
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.WordSpec
import io.kotest.matchers.shouldBe
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
   "Enum with documentation and aliases" should {
      @Serializable
      data class EnumWithDocuTest(
         val value: Suit
      )

      val expected =
         org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/enum_with_documentation.json"))
      val schema = Avro.default.schema(EnumWithDocuTest.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }

   "Enum with default values" should {
      "generate schema" {
         @Serializable
         data class EnumWithDefaultTest(
            val type: IngredientType
         )

         val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/enum_with_default.json"))

         val schema = Avro.default.schema(EnumWithDefaultTest.serializer())

         schema.toString(true) shouldBe expected.toString(true)
      }
      "generate schema with default and nullable union types" {
         @Serializable
         data class EnumWithDefaultTest(
            @AvroDefault(Avro.NULL) val type: IngredientType?
         )

         val expected =
            org.apache.avro.Schema.Parser()
               .parse(javaClass.getResourceAsStream("/enum_with_default_value_and_null.json"))

         val schema = Avro.default.schema(EnumWithDefaultTest.serializer())

         schema.toString(true) shouldBe expected.toString(true)
      }
      "fail with unknown values" {
         @Serializable
         data class EnumWithDefaultTest(
            val type: InvalidIngredientType
         )
         shouldThrow<IllegalStateException> {
            Avro.default.schema(EnumWithDefaultTest.serializer())
         }
      }
   }
})

enum class Wine {
   Malbec, Shiraz, CabSav, Merlot
}

@Serializable
@AvroAliases(["MySuit"])
@AvroDoc("documentation")
enum class Suit {
   SPADES, HEARTS, DIAMONDS, CLUBS;
}

@Serializable
@AvroEnumDefault("MEAT")
enum class IngredientType { VEGGIE, MEAT, }

@Serializable
@AvroEnumDefault("PINEAPPLE")
enum class InvalidIngredientType { VEGGIE, MEAT, }
