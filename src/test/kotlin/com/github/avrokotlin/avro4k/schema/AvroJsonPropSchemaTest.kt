package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroJsonProp
import io.kotest.core.spec.style.WordSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable

class AvroJsonPropSchemaTest : WordSpec() {
    enum class Colours {
        Red,
        Green,
        Blue,
    }

    init {
        "@AvroJsonProp" should {
            "support props annotation on class" {

                val expected =
                    org.apache.avro.Schema.Parser()
                        .parse(javaClass.getResourceAsStream("/props_json_annotation_class.json"))
                val schema = Avro.default.schema(TypeAnnotated.serializer())
                schema.toString(true) shouldBe expected.toString(true)
            }
            "support props annotation on field" {

                val expected =
                    org.apache.avro.Schema.Parser()
                        .parse(javaClass.getResourceAsStream("/props_json_annotation_field.json"))
                val schema = Avro.default.schema(AnnotatedProperties.serializer())
                schema.toString(true) shouldBe expected.toString(true)
            }
            "support props annotations on enums" {

                val expected =
                    org.apache.avro.Schema.Parser()
                        .parse(javaClass.getResourceAsStream("/props_json_annotation_scala_enum.json"))
                val schema = Avro.default.schema(EnumAnnotated.serializer())
                schema.toString(true) shouldBe expected.toString(true)
            }
        }
    }

    @Serializable
    @AvroJsonProp("guns", """["and", "roses"]""")
    data class TypeAnnotated(val str: String)

    @Serializable
    data class AnnotatedProperties(
        @AvroJsonProp("guns", """["and", "roses"]""") val str: String,
        @AvroJsonProp("jean", """["michel", "jarre"]""") val long: Long,
        @AvroJsonProp(
            key = "object",
            jsonValue = """{
           "a": "foo",
           "b": 200,
           "c": true,
           "d": null,
           "e": { "e1": null, "e2": 429 },
           "f": ["bar", 404, false, null, {}]
         }"""
        )
        val int: Int,
    )

    @Serializable
    data class EnumAnnotated(
        @AvroJsonProp("guns", """["and", "roses"]""") val colours: Colours,
    )
}