package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroDoc
import io.kotest.core.spec.style.WordSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable

class AvroDocSchemaTest : WordSpec({

    "@AvroDoc" should {
        "support doc annotation on class" {

            val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/doc_annotation_class.json"))
            val schema = Avro.default.schema(TypeAnnotated.serializer())
            schema.toString(true) shouldBe expected.toString(true)
        }
        "support doc annotation on field" {

            val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/doc_annotation_field.json"))
            val schema = Avro.default.schema(FieldAnnotated.serializer())
            schema.toString(true) shouldBe expected.toString(true)
        }
        "support doc annotation on nested class" {

            val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/doc_annotation_field_struct.json"))
            val schema = Avro.default.schema(NestedAnnotated.serializer())
            schema.toString(true) shouldBe expected.toString(true)
        }
    }
}) {
    @AvroDoc("hello; is it me youre looking for")
    @Serializable
    data class TypeAnnotated(val str: String)

    @Serializable
    data class FieldAnnotated(
        @AvroDoc("hello its me") val str: String,
        @AvroDoc("I am a long") val long: Long,
        val int: Int,
    )

    @Serializable
    data class Nested(
        @AvroDoc("b") val foo: String,
    )

    @Serializable
    data class NestedAnnotated(
        @AvroDoc("c") val nested: Nested,
    )
}