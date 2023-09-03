package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.ListRecord
import com.github.avrokotlin.avro4k.shouldBeContentOf
import io.kotest.core.spec.style.WordSpec
import kotlinx.serialization.Serializable
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8

class ArrayEncoderTest : WordSpec({

   "Encoder" should {
      "generate GenericData.Array for an Array<Boolean>" {

          val schema = Avro.default.schema(ArrayBooleanTest.serializer())
          val arraySchema = schema.getField("a").schema()
          val record = Avro.default.encode(ArrayBooleanTest.serializer(), ArrayBooleanTest(arrayOf(true, false, true)))
          record shouldBeContentOf ListRecord(schema, listOf(GenericData.Array(arraySchema, listOf(true, false, true))))
      }
      "support GenericData.Array for an Array<Boolean> with other fields" {

          val schema = Avro.default.schema(ArrayBooleanWithOthersTest.serializer())
          val arraySchema = schema.getField("b").schema()
          val record = Avro.default.encode(ArrayBooleanWithOthersTest.serializer(), ArrayBooleanWithOthersTest("foo", arrayOf(true, false, true), 123L))
          record shouldBeContentOf ListRecord(
                  schema,
                  Utf8("foo"),
                  GenericData.Array(arraySchema, listOf(true, false, true)),
                  123L
          )
      }
      "generate GenericData.Array for a List<String>" {

          val schema = Avro.default.schema(ListStringTest.serializer())
          val arraySchema = schema.getField("a").schema()
          val record = Avro.default.encode(ListStringTest.serializer(), ListStringTest(listOf("we23", "54z")))
          record shouldBeContentOf ListRecord(
                  schema,
                  listOf(GenericData.Array(arraySchema, listOf(Utf8("we23"), Utf8("54z"))))
          )
      }
      "generate GenericData.Array for a Set<Long>" {

          val schema = Avro.default.schema(SetLongTest.serializer())
          val arraySchema = schema.getField("a").schema()
          val expected = ListRecord(schema, listOf(GenericData.Array(arraySchema, listOf(123L, 643L, 912))))
          val actual = Avro.default.encode(SetLongTest.serializer(), SetLongTest(setOf(123L, 643L, 912L)))
          actual shouldBeContentOf expected
      }
//    "generate array for an Array of records" {
//      @Serializable
//      data class Nested(val goo: String)
//
//      @Serializable
//      data class Test(val array: Array<Nested>)
//
//      val schema = Avro.default.schema(Test.serializer())
//      val nestedSchema = Avro.default.schema(Nested.serializer())
//
//      val record = Avro.default.toRecord(Test.serializer(), Test(arrayOf(Nested("qwe"), Nested("dfsg"))))
//      record shouldBe ImmutableRecord(
//          schema,
//          arrayListOf(
//              arrayListOf(
//                  ImmutableRecord(nestedSchema, arrayListOf(Utf8("qwe"))),
//                  ImmutableRecord(nestedSchema, arrayListOf(Utf8("dfsg")))
//              )
//          )
//      )
//    }
//    "generate array for a List of records" {
//      @Serializable
//      data class Nested(val goo: String)
//
//      @Serializable
//      data class Test(val list: List<Nested>)
//
//      val schema = Avro.default.schema(Test.serializer())
//      val nestedSchema = Avro.default.schema(Nested.serializer())
//
//      val record = Avro.default.toRecord(Test.serializer(), Test(listOf(Nested("qwe"), Nested("dfsg"))))
//      record shouldBe ImmutableRecord(
//          schema,
//          arrayListOf(
//              arrayListOf(
//                  ImmutableRecord(nestedSchema, arrayListOf(Utf8("qwe"))),
//                  ImmutableRecord(nestedSchema, arrayListOf(Utf8("dfsg")))
//              )
//          )
//      )
//    }
//    "generate array for a Set of records" {
//      @Serializable
//      data class Nested(val goo: String)
//
//      @Serializable
//      data class Test(val set: Set<Nested>)
//
//      val schema = Avro.default.schema(Test.serializer())
//      val nestedSchema = Avro.default.schema(Nested.serializer())
//
//      val record = Avro.default.toRecord(Test.serializer(), Test(setOf(Nested("qwe"), Nested("dfsg"))))
//      record shouldBe ImmutableRecord(
//          schema,
//          arrayListOf(
//              arrayListOf(
//                  ImmutableRecord(nestedSchema, arrayListOf(Utf8("qwe"))),
//                  ImmutableRecord(nestedSchema, arrayListOf(Utf8("dfsg")))
//              )
//          )
//      )
//    }
   }
}) {
   @Serializable
   data class ArrayBooleanTest(val a: Array<Boolean>)

   @Serializable
   data class ArrayBooleanWithOthersTest(val a: String, val b: Array<Boolean>, val c: Long)

   @Serializable
   data class SetLongTest(val a: Set<Long>)

   @Serializable
   data class ListStringTest(val a: List<String>)
}
