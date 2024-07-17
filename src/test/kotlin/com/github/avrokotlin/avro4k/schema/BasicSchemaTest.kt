package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.AvroAssertions
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.Serializable
import kotlin.io.path.Path

internal class BasicSchemaTest : FunSpec({

    test("schema for basic types") {
        AvroAssertions.assertThat<Foo>()
            .generatesSchema(Path("/basic.json"))
    }

    test("accept nested case classes") {
        AvroAssertions.assertThat<Test>()
            .generatesSchema(Path("/nested.json"))
    }

    test("accept multiple nested case classes") {
        AvroAssertions.assertThat<Outer>()
            .generatesSchema(Path("/nested_multiple.json"))
    }

    test("accept deep nested structure") {
        AvroAssertions.assertThat<Level1>()
            .generatesSchema(Path("/deepnested.json"))
    }
}) {
    @Serializable
    private data class Nested(val goo: String)

    @Serializable
    private data class Test(val foo: String, val nested: Nested)

    @Serializable
    private data class Foo(
        val a: String,
        val b: Double,
        val c: Boolean,
        val d: Float,
        val e: Long,
        val f: Int,
        val g: Short,
        val h: Byte,
    )

    @Serializable
    private data class Inner(val goo: String)

    @Serializable
    private data class Middle(val inner: Inner)

    @Serializable
    private data class Outer(val middle: Middle)

    @Serializable
    private data class Level4(val str: String)

    @Serializable
    private data class Level3(val level4: Level4)

    @Serializable
    private data class Level2(val level3: Level3)

    @Serializable
    private data class Level1(val level2: Level2)
}