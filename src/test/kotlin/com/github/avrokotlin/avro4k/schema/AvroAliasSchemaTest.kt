package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.AvroAlias
import com.github.avrokotlin.avro4k.AvroAssertions
import io.kotest.core.spec.style.WordSpec
import kotlinx.serialization.Serializable
import kotlin.io.path.Path

class AvroAliasSchemaTest : WordSpec({

    "SchemaEncoder" should {
        "support alias annotations on types" {
            AvroAssertions.assertThat<TypeAnnotated>()
                .generatesSchema(Path("/aliases_on_types.json"))
        }
        "support multiple alias annotations on types" {
            AvroAssertions.assertThat<TypeAliasAnnotated>()
                .generatesSchema(Path("/aliases_on_types_multiple.json"))
        }
        "support alias annotations on field" {
            AvroAssertions.assertThat<FieldAnnotated>()
                .generatesSchema(Path("/aliases_on_fields.json"))
        }
        "support multiple alias annotations on fields" {
            AvroAssertions.assertThat<FieldAliasAnnotated>()
                .generatesSchema(Path("/aliases_on_fields_multiple.json"))
        }
    }
}) {
    @Serializable
    @AvroAlias("queen")
    private data class TypeAnnotated(val str: String)

    @AvroAlias("queen", "ledzep")
    @Serializable
    private data class TypeAliasAnnotated(val str: String)

    @Serializable
    private data class FieldAnnotated(
        @AvroAlias("cold") val str: String,
        @AvroAlias("kate") val long: Long,
        val int: IntValue,
    )

    @Serializable
    @JvmInline
    private value class IntValue(
        @AvroAlias("ignoredAlias") val value: Int,
    )

    @Serializable
    private data class FieldAliasAnnotated(
        @AvroAlias("queen", "ledzep") val str: String,
    )
}