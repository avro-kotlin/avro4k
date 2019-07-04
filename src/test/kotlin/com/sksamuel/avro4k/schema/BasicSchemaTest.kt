package com.sksamuel.avro4k.schema

import com.sksamuel.avro4k.Avro
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import kotlinx.serialization.Serializable


@Serializable
data class Nested(val goo: String)

@Serializable
data class Test(val foo: String, val nested: Nested)

//@Serializable
//data class RecursiveFoo(val list: List<RecursiveFoo>)


@Suppress("BlockingMethodInNonBlockingContext")
class BasicSchemaTest : FunSpec({

  test("schema for basic types") {

    @Serializable
    data class Foo(val a: String,
                   val b: Double,
                   val c: Boolean,
                   val d: Float,
                   val e: Long,
                   val f: Int,
                   val g: Short,
                   val h: Byte)


    val expected = org.apache.avro.Schema.Parser().parse(this::class.java.getResourceAsStream("/basic.json"))
    val schema = Avro.default.schema(Foo.serializer())
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("accept nested case classes") {
    val expected = org.apache.avro.Schema.Parser().parse(this::class.java.getResourceAsStream("/nested.json"))
    val schema = Avro.default.schema(Test.serializer())
    schema.toString(true) shouldBe expected.toString(true)
  }

//  test("!support simple recursive types") {
//    val schema = Avro.default.schema(RecursiveFoo.serializer())
//    val expected = org.apache.avro.Schema.Parser().parse(this::class.java.getResourceAsStream("/recursive.json"))
//    schema.toString(true) shouldBe expected.toString(true)
//  }

  test("accept multiple nested case classes") {

    @Serializable
    data class Inner(val goo: String)

    @Serializable
    data class Middle(val inner: Inner)

    @Serializable
    data class Outer(val middle: Middle)

    val expected = org.apache.avro.Schema.Parser().parse(this::class.java.getResourceAsStream("/nested_multiple.json"))
    val schema = Avro.default.schema(Outer.serializer())
    schema.toString(true) shouldBe expected.toString(true)
  }

  test("accept deep nested structure") {

    @Serializable
    data class Level4(val str: String)

    @Serializable
    data class Level3(val level4: Level4)

    @Serializable
    data class Level2(val level3: Level3)

    @Serializable
    data class Level1(val level2: Level2)

    val expected = org.apache.avro.Schema.Parser().parse(this::class.java.getResourceAsStream("/deepnested.json"))
    val schema = Avro.default.schema(Level1.serializer())
    schema.toString(true) shouldBe expected.toString(true)
  }
})