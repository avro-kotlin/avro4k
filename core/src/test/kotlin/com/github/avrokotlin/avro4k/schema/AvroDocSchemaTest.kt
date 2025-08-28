package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.AvroDoc
import io.kotest.core.spec.style.WordSpec
import kotlinx.serialization.Serializable
import kotlin.io.path.Path

internal class AvroDocSchemaTest : WordSpec({

    "@AvroDoc" should {
        "support doc annotation on class" {
            AvroAssertions.assertThat<TypeAnnotated>()
                .generatesSchema(Path("/doc_annotation_class.json"))
        }
        "support doc annotation on field" {
            AvroAssertions.assertThat<FieldAnnotated>()
                .generatesSchema(Path("/doc_annotation_field.json"))
        }
        "support doc annotation on nested class" {
            AvroAssertions.assertThat<NestedAnnotated>()
                .generatesSchema(Path("/doc_annotation_field_struct.json"))
        }
    }
}) {
    @AvroDoc("hello; is it me youre looking for")
    @Serializable
    private data class TypeAnnotated(val str: String)

    @Serializable
    private data class FieldAnnotated(
        @AvroDoc("hello its me") val str: String,
        @AvroDoc("I am a long") val long: Long,
        val int: Int,
    )

    @Serializable
    private data class Nested(
        @AvroDoc("b") val foo: String,
    )

    @Serializable
    private data class NestedAnnotated(
        @AvroDoc("c") val nested: Nested,
    )
}