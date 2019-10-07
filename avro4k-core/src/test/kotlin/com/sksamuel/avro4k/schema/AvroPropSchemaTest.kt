package com.sksamuel.avro4k.schema

import com.sksamuel.avro4k.Avro
import com.sksamuel.avro4k.AvroProp
import io.kotlintest.shouldBe
import io.kotlintest.specs.WordSpec
import kotlinx.serialization.Serializable

class AvroPropSchemaTest : WordSpec() {

  enum class Colours {
    Red, Green, Blue
  }

  init {
    "@AvroProp" should {
      "support prop annotation on class"  {

        @Serializable
        @AvroProp("cold", "play")
        data class Annotated(val str: String)

        val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/props_annotation_class.json"))
        val schema = Avro.default.schema(Annotated.serializer())
        schema.toString(true) shouldBe expected.toString(true)
      }
      "support prop annotation on field" {

        @Serializable
        data class AnnotatedProperties(
            @AvroProp("cold", "play") val str: String,
            @AvroProp("kate", "bush") val long: Long,
            val int: Int)

        val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/props_annotation_field.json"))
        val schema = Avro.default.schema(AnnotatedProperties.serializer())
        schema.toString(true) shouldBe expected.toString(true)
      }
      "support props annotations on enums"  {

        @Serializable
        data class Annotated(@AvroProp("cold", "play") val colours: Colours)

        val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/props_annotation_scala_enum.json"))
        val schema = Avro.default.schema(Annotated.serializer())
        schema.toString(true) shouldBe expected.toString(true)
      }
    }
  }
}