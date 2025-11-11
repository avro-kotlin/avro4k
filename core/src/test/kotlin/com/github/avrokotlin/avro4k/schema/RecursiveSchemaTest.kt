package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.AvroAssertions
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.Serializable
import kotlin.io.path.Path

internal class RecursiveSchemaTest : FunSpec({

    test("accept direct recursive classes") {
        AvroAssertions.assertThat<RecursiveClass>()
            .generatesSchema(Path("/recursive_class.json"))
    }

    test("accept direct recursive lists") {
        AvroAssertions.assertThat<RecursiveListItem>()
            .generatesSchema(Path("/recursive_list.json"))
    }

    test("accept direct recursive maps") {
        AvroAssertions.assertThat<RecursiveMapValue>()
            .generatesSchema(Path("/recursive_map.json"))
    }

    test("accept nested recursive classes") {
        AvroAssertions.assertThat<Level1>()
            .generatesSchema(Path("/recursive_nested.json"))
    }
}) {
    @Serializable
    private data class RecursiveClass(val payload: Int, val klass: RecursiveClass?)

    @Serializable
    private data class RecursiveListItem(val payload: Int, val list: List<RecursiveListItem>?)

    @Serializable
    private data class RecursiveMapValue(val payload: Int, val map: Map<String, RecursiveMapValue>?)

    @Serializable
    private data class Level3(val level1: Level1?)

    @Serializable
    private data class Level2(val level3: Level3)

    @Serializable
    private data class Level1(val level2: Level2?)
}