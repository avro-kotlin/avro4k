package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroProp
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.WordSpec
import kotlinx.serialization.Serializable

class AvroPropSchemaTest : WordSpec() {

  enum class Colours {
    Red, Green, Blue
  }

  init {
    "@AvroProp" should {
      "support prop annotation on class"  {

        val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/props_annotation_class.json"))
        val schema = Avro.default.schema(TypeAnnotated.serializer())
        schema.toString(true) shouldBe expected.toString(true)
      }
      "support prop annotation on field" {

        val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/props_annotation_field.json"))
        val schema = Avro.default.schema(AnnotatedProperties.serializer())
        schema.toString(true) shouldBe expected.toString(true)
      }
      "support props annotations on enums"  {

        val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/props_annotation_scala_enum.json"))
        val schema = Avro.default.schema(EnumAnnotated.serializer())
        schema.toString(true) shouldBe expected.toString(true)
      }
    }
  }

  @Serializable
  @AvroProp("cold", "play")
  data class TypeAnnotated(val str: String)

  @Serializable
  data class AnnotatedProperties(
      @AvroProp("cold", "play") val str: String,
      @AvroProp("kate", "bush") val long: Long,
      val int: Int)

  @Serializable
  data class EnumAnnotated(@AvroProp("cold", "play") val colours: Colours)
}
