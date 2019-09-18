package com.sksamuel.avro4k.schema

import com.sksamuel.avro4k.Avro
import io.kotlintest.shouldBe
import io.kotlintest.specs.WordSpec
import kotlinx.serialization.Serializable

class ArraySchemaTest : WordSpec({

  "SchemaEncoder" should {
    "generate array type for an Array of primitives" {
      @Serializable
      data class Test(val array: Array<Boolean>)

      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/array.json"))
      val schema = Avro.default.schema(Test.serializer())
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate array type for a List of primitives" {
      @Serializable
      data class NestedListString(val list: List<String>)

      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/list.json"))
      val schema = Avro.default.schema(NestedListString.serializer())
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate array type for an Array of records" {

      @Serializable
      data class Nested(val goo: String)

      @Serializable
      data class Test(val array: Array<Nested>)

      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/arrayrecords.json"))
      val schema = Avro.default.schema(Test.serializer())
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate array type for a List of records" {
      @Serializable
      data class Nested(val goo: String)

      @Serializable
      data class Test(val list: List<Nested>)

      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/listrecords.json"))
      val schema = Avro.default.schema(Test.serializer())
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate array type for a Set of records" {

      @Serializable
      data class Nested(val goo: String)

      @Serializable
      data class NestedSet(val set: Set<Nested>)

      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/setrecords.json"))
      val schema = Avro.default.schema(NestedSet.serializer())
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate array type for a Set of strings" {
      @Serializable
      data class Test(val set: Set<String>)

      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/setstrings.json"))
      val schema = Avro.default.schema(Test.serializer())
      schema.toString(true) shouldBe expected.toString(true)
    }
    "generate array type for a Set of doubles" {
      @Serializable
      data class NestedSetDouble(val set: Set<Double>)

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
//    "support array of maps" {
//      data class Test(array: Array[Map[String, String]])
//      val expected = org . apache . avro . Schema . Parser ().parse(javaClass.getResourceAsStream("/array_of_maps.json"))
//      val schema = AvroSchema[Test]
//      schema.toString(true) shouldBe expected.toString(true)
//    }
//    "support lists of maps" {
//      data class Test(list: List[Map[String, String]])
//      val expected = org . apache . avro . Schema . Parser ().parse(javaClass.getResourceAsStream("/list_of_maps.json"))
//      val schema = AvroSchema[Test]
//      schema.toString(true) shouldBe expected.toString(true)
//    }
//    "support seq of maps" {
//      data class Test(seq: Seq[Map[String, String]])
//      val expected = org . apache . avro . Schema . Parser ().parse(javaClass.getResourceAsStream("/seq_of_maps.json"))
//      val schema = AvroSchema[Test]
//      schema.toString(true) shouldBe expected.toString(true)
//    }
//    "support vector of maps" {
//      data class Test(vector: Vector[Map[String, String]])
//      val expected = org . apache . avro . Schema . Parser ().parse(javaClass.getResourceAsStream("/vector_of_maps.json"))
//      val schema = AvroSchema[Test]
//      schema.toString(true) shouldBe expected.toString(true)
//    }
//    "support data class of seq of data class with maps" {
//      data class Ship(map: scala.collection.immutable.Map[String, String])
//      data class Test(ship: List[scala.collection.immutable.Map[String, String]])
//      val expected = org . apache . avro . Schema . Parser ().parse(javaClass.getResourceAsStream("/class_of_list_of_maps.json"))
//      val schema = AvroSchema[Test]
//      schema.toString(true) shouldBe expected.toString(true)
//    }
  }

})