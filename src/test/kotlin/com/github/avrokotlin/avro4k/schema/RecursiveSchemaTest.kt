package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.Avro
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable

@Serializable
data class RecursiveClass(val payload: Int, val klass: RecursiveClass?)

@Serializable
data class RecursiveListItem(val payload: Int, val list: List<RecursiveListItem>?)

@Serializable
data class RecursiveMapValue(val payload: Int, val map: Map<String, RecursiveMapValue>?)

@Serializable
data class Level4(val level1: Level1)

@Serializable
data class Level3(val level4: Level4?)

@Serializable
data class Level2(val level3: Level3)

@Serializable
data class Level1(val level2: Level2?)

class RecursiveSchemaTest : FunSpec({

    test("accept direct recursive classes") {
        val expected = org.apache.avro.Schema.Parser().parse(this::class.java.getResourceAsStream("/recursive_class.json"))
        val schema = Avro.default.schema(RecursiveClass.serializer())
        schema.toString(true) shouldBe expected.toString(true)
    }

    test("accept direct recursive lists") {
        val expected = org.apache.avro.Schema.Parser().parse(this::class.java.getResourceAsStream("/recursive_list.json"))
        val schema = Avro.default.schema(RecursiveListItem.serializer())
        schema.toString(true) shouldBe expected.toString(true)
    }

    test("accept direct recursive maps") {
        val expected = org.apache.avro.Schema.Parser().parse(this::class.java.getResourceAsStream("/recursive_map.json"))
        val schema = Avro.default.schema(RecursiveMapValue.serializer())
        schema.toString(true) shouldBe expected.toString(true)
    }

    test("accept nested recursive classes") {
        val expected = org.apache.avro.Schema.Parser().parse(this::class.java.getResourceAsStream("/recursive_nested.json"))
        val schema = Avro.default.schema(Level1.serializer())
        schema.toString(true) shouldBe expected.toString(true)
    }
})