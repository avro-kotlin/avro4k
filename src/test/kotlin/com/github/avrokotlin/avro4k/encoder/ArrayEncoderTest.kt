package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.ListRecord
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.WordSpec
import kotlinx.serialization.Serializable
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8

class ArrayEncoderTest : WordSpec({

   "Encoder" should {
      "generate GenericData.Array for an Array<Boolean>" {
         @Serializable
         data class Test(val a: Array<Boolean>)

         val schema = Avro.default.schema(Test.serializer())
         val arraySchema = schema.getField("a").schema()
         val record = Avro.default.toRecord(Test.serializer(), Test(arrayOf(true, false, true)))
         record shouldBe ListRecord(schema, listOf(GenericData.Array(arraySchema, listOf(true, false, true))))
      }
      "support GenericData.Array for an Array<Boolean> with other fields" {
         @Serializable
         data class Test(val a: String, val b: Array<Boolean>, val c: Long)

         val schema = Avro.default.schema(Test.serializer())
         val arraySchema = schema.getField("b").schema()
         val record = Avro.default.toRecord(Test.serializer(), Test("foo", arrayOf(true, false, true), 123L))
         record shouldBe ListRecord(
            schema,
            Utf8("foo"),
            GenericData.Array(arraySchema, listOf(true, false, true)),
            123L
         )
      }
      "generate GenericData.Array for a List<String>" {
         @Serializable
         data class Test(val a: List<String>)

         val schema = Avro.default.schema(Test.serializer())
         val arraySchema = schema.getField("a").schema()
         val record = Avro.default.toRecord(Test.serializer(), Test(listOf("we23", "54z")))
         record shouldBe ListRecord(
            schema,
            listOf(GenericData.Array(arraySchema, listOf(Utf8("we23"), Utf8("54z"))))
         )
      }
      "generate GenericData.Array for a Set<Long>" {
         @Serializable
         data class Test(val a: Set<Long>)

         val schema = Avro.default.schema(Test.serializer())
         val arraySchema = schema.getField("a").schema()
         val expected = ListRecord(schema, listOf(GenericData.Array(arraySchema, listOf(123L, 643L, 912))))
         val actual = Avro.default.toRecord(Test.serializer(), Test(setOf(123L, 643L, 912L)))
         actual shouldBe expected
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
})

