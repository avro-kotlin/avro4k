package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroAliases
import com.github.avrokotlin.avro4k.AvroDefault
import com.github.avrokotlin.avro4k.AvroDoc
import com.github.avrokotlin.avro4k.AvroEnumDefault
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.WordSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable

class EnumSchemaTest : WordSpec({

   "SchemaEncoder" should {
      "accept enums" {

         val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/enum.json"))
         val schema = Avro.default.schema(EnumTest.serializer())
         schema.toString(true) shouldBe expected.toString(true)
      }
   }
   "Enum with documentation and aliases" should {

      val expected =
         org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/enum_with_documentation.json"))
      val schema = Avro.default.schema(EnumWithDocuTest.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }

   "Enum with default values" should {
      "generate schema" {

         val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/enum_with_default.json"))

         val schema = Avro.default.schema(EnumWithDefaultTest.serializer())

         schema.toString(true) shouldBe expected.toString(true)
      }
      "generate schema with default and nullable union types" {

         val expected =
            org.apache.avro.Schema.Parser()
               .parse(javaClass.getResourceAsStream("/enum_with_default_value_and_null.json"))

         val schema = Avro.default.schema(EnumWithAvroDefaultTest.serializer())

         schema.toString(true) shouldBe expected.toString(true)
      }
      "modifying namespaces retains enum defaults" {
         val schemaWithNewNameSpace = Avro.default.schema(EnumWithDefaultTest.serializer()).overrideNamespace("new")

         val expected = org.apache.avro.Schema.Parser()
            .parse(javaClass.getResourceAsStream("/enum_with_default_new_namespace.json"))

         schemaWithNewNameSpace.toString(true) shouldBe expected.toString(true)
      }
      "fail with unknown values" {
         shouldThrow<IllegalStateException> {
            Avro.default.schema(EnumWithUnknownDefaultTest.serializer())
         }
      }
   }
}) {

   @Serializable
   data class EnumTest(val wine: Wine)
   @Serializable
   data class EnumWithDocuTest(
       val value: Suit
   )
   @Serializable
   data class EnumWithDefaultTest(
       val type: IngredientType
   )
   @Serializable
   data class EnumWithAvroDefaultTest(
       @AvroDefault(Avro.NULL) val type: IngredientType?
   )
   @Serializable
   data class EnumWithUnknownDefaultTest(
       val type: InvalidIngredientType
   )

}

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
