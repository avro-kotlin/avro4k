package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.AvroAssertions
import io.kotest.core.spec.style.WordSpec
import kotlinx.serialization.Serializable
import kotlin.io.path.Path

internal class ArraySchemaTest : WordSpec({

    "SchemaEncoder" should {
        "generate array type for an Array of primitives" {
            AvroAssertions.assertThat<BooleanArrayTest>()
                .generatesSchema(Path("/array.json"))
        }
        "generate array type for a List of primitives" {
            AvroAssertions.assertThat<NestedListString>()
                .generatesSchema(Path("/list.json"))
        }
        "generate array type for an Array of records" {
            AvroAssertions.assertThat<NestedArrayTest>()
                .generatesSchema(Path("/arrayrecords.json"))
        }
        "generate array type for a List of records" {
            AvroAssertions.assertThat<NestedListTest>()
                .generatesSchema(Path("/listrecords.json"))
        }
        "generate array type for a Set of records" {
            AvroAssertions.assertThat<NestedSet>()
                .generatesSchema(Path("/setrecords.json"))
        }
        "generate array type for a Set of strings" {
            AvroAssertions.assertThat<StringSetTest>()
                .generatesSchema(Path("/setstrings.json"))
        }
        "generate array type for a Set of doubles" {
            AvroAssertions.assertThat<NestedSetDouble>()
                .generatesSchema(Path("/setdoubles.json"))
        }
    }
}) {
    @Serializable
    private data class BooleanArrayTest(val array: Array<Boolean>)

    @Serializable
    private data class Nested(val goo: String)

    @Serializable
    private data class NestedListString(val list: List<String>)

    @Serializable
    private data class NestedArrayTest(val array: Array<Nested>)

    @Serializable
    private data class NestedListTest(val list: List<Nested>)

    @Serializable
    private data class NestedSet(val set: Set<Nested>)

    @Serializable
    private data class StringSetTest(val set: Set<String>)

    @Serializable
    private data class NestedSetDouble(val set: Set<Double>)
}