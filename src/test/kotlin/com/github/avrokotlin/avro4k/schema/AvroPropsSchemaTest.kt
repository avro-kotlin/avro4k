package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.AvroEnumDefault
import com.github.avrokotlin.avro4k.AvroProp
import io.kotest.core.spec.style.StringSpec
import kotlinx.serialization.Serializable
import kotlin.io.path.Path

class AvroPropsSchemaTest : StringSpec({
    "should support props annotation on class" {
        AvroAssertions.assertThat<TypeAnnotated>()
            .generatesSchema(Path("/props_json_annotation_class.json"))
    }
}) {
    @Serializable
    @AvroProp("hey", "there")
    @AvroProp("counting", """["one", "two"]""")
    private data class TypeAnnotated(
        @AvroProp("cold", "play")
        @AvroProp(
            key = "complexObject",
            value = """{
           "a": "foo",
           "b": 200,
           "c": true,
           "d": null,
           "e": { "e1": null, "e2": 429 },
           "f": ["bar", 404, false, null, {}]
         }"""
        )
        val field: EnumAnnotated,
    )

    @Serializable
    @AvroProp("enums", "power")
    @AvroProp("countingAgain", """["three", "four"]""")
    private enum class EnumAnnotated {
        Red,

        @AvroEnumDefault
        Green,
        Blue,
    }
}