package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.AvroFixed
import io.kotest.core.spec.style.WordSpec
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.io.path.Path

class AvroFixedSchemaTest : WordSpec({
    "@AvroFixed" should {
        "generated fixed field schema when used on a field" {
            AvroAssertions.assertThat<FixedStringField>()
                .generatesSchema(Path("/fixed_string.json"))
        }

        "generated fixed field schema when used on a value class' field" {
            AvroAssertions.assertThat<FixedNestedStringField>()
                .generatesSchema(Path("/fixed_string.json"))
        }

        "generated fixed field schema with @AvroFixed from class field instead of value class' field" {
            AvroAssertions.assertThat<FieldPriorToValueClass>()
                .generatesSchema(Path("/fixed_string_5.json"))
        }
    }
}) {
    @Serializable
    @SerialName("Fixed")
    private data class FixedStringField(
        @AvroFixed(7) val mystring: String,
    )

    @Serializable
    @SerialName("Fixed")
    private data class FixedNestedStringField(
        val mystring: FixedString,
    )

    @Serializable
    @SerialName("Fixed")
    private data class FieldPriorToValueClass(
        @AvroFixed(5) val mystring: FixedString,
    )

    @JvmInline
    @Serializable
    @SerialName("FixedString")
    private value class FixedString(
        @AvroFixed(7) val mystring: String,
    )
}