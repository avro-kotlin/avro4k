package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.Avro
import io.kotest.core.spec.style.WordSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable

class ArraySchemaTest : WordSpec({

    "SchemaEncoder" should {
        "generate array type for an Array of primitives" {

            val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/array.json"))
            val schema = Avro.default.schema(BooleanArrayTest.serializer())
            schema.toString(true) shouldBe expected.toString(true)
        }
        "generate array type for a List of primitives" {

            val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/list.json"))
            val schema = Avro.default.schema(NestedListString.serializer())
            schema.toString(true) shouldBe expected.toString(true)
        }
        "generate array type for an Array of records" {

            val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/arrayrecords.json"))
            val schema = Avro.default.schema(NestedArrayTest.serializer())
            schema.toString(true) shouldBe expected.toString(true)
        }
        "generate array type for a List of records" {

            val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/listrecords.json"))
            val schema = Avro.default.schema(NestedListTest.serializer())
            schema.toString(true) shouldBe expected.toString(true)
        }
        "generate array type for a Set of records" {

            val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/setrecords.json"))
            val schema = Avro.default.schema(NestedSet.serializer())
            schema.toString(true) shouldBe expected.toString(true)
        }
        "generate array type for a Set of strings" {

            val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/setstrings.json"))
            val schema = Avro.default.schema(StringSetTest.serializer())
            schema.toString(true) shouldBe expected.toString(true)
        }
        "generate array type for a Set of doubles" {

            val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/setdoubles.json"))
            val schema = Avro.default.schema(NestedSetDouble.serializer())
            schema.toString(true) shouldBe expected.toString(true)
        }
//    "support top level List[Int]" {
//      @Serializable
//      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/top_level_list_int.json"))
//      val schema = AvroSchema[List[Int]]
//      val schema = Avro.default.schema(Test.serializer())
//      schema.toString(true) shouldBe expected.toString(true)
//    }
//    "support top level Set[Boolean]" {
//      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/top_level_set_boolean.json"))
//      val schema = AvroSchema[Set[Boolean]]
//      val schema = Avro.default.schema(Test.serializer())
//      schema.toString(true) shouldBe expected.toString(true)
//    }
    }
}) {
    @Serializable
    data class BooleanArrayTest(val array: Array<Boolean>)

    @Serializable
    data class Nested(val goo: String)

    @Serializable
    data class NestedListString(val list: List<String>)

    @Serializable
    data class NestedArrayTest(val array: Array<Nested>)

    @Serializable
    data class NestedListTest(val list: List<Nested>)

    @Serializable
    data class NestedSet(val set: Set<Nested>)

    @Serializable
    data class StringSetTest(val set: Set<String>)

    @Serializable
    data class NestedSetDouble(val set: Set<Double>)
}