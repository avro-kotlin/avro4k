package com.sksamuel.avro4k.schema

import com.sksamuel.avro4k.Avro
import com.sksamuel.avro4k.io.AvroInputStream
import com.sksamuel.avro4k.io.AvroOutputStream
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.Serializable
import java.io.File

@Serializable
data class Recursive(val payload: Int, val next: Recursive?)

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
    val expected = org.apache.avro.Schema.Parser().parse(this::class.java.getResourceAsStream("/recursive.json"))
    val schema = Avro.default.schema(Recursive.serializer())
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("accept nested recursive classes") {
    val expected = org.apache.avro.Schema.Parser().parse(this::class.java.getResourceAsStream("/recursive_nested.json"))
    val schema = Avro.default.schema(Level1.serializer())
    schema.toString(true) shouldBe expected.toString(true)
  }
})
