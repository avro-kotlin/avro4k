package com.sksamuel.avro4k.encoder

import com.sksamuel.avro4k.Avro
import com.sksamuel.avro4k.ImmutableRecord
import io.kotlintest.shouldBe
import io.kotlintest.specs.WordSpec
import kotlinx.serialization.Serializable
import org.apache.avro.util.Utf8

class ArrayEncoderTest : WordSpec({

  "Encoder" should {
//    "generate array for an Array of primitives" {
//      @Serializable
//      data class Test(val array: Array<Boolean>)
//
//      val schema = Avro.default.schema(Test.serializer())
//      val record = Avro.default.toRecord(Test.serializer(), Test(arrayOf(true, false, true)))
//      record shouldBe ImmutableRecord(schema, arrayListOf(arrayListOf(true, false, true)))
//    }
    "support array for an Array of primitives with other fields" {
      @Serializable
      data class Test(val a: String, val array: Array<Boolean>, val c: Long)

      val schema = Avro.default.schema(Test.serializer())
      val record = Avro.default.toRecord(Test.serializer(), Test("wibble", arrayOf(true, false, true), 123L))
      record shouldBe ImmutableRecord(schema, arrayListOf(Utf8("wibble"), arrayListOf(true, false, true), 123L))
    }
//    "generate array for a List of primitives" {
//      @Serializable
//      data class Test(val list: List<String>)
//
//      val schema = Avro.default.schema(Test.serializer())
//      val record = Avro.default.toRecord(Test.serializer(), Test(listOf("we23", "54")))
//      record shouldBe ImmutableRecord(schema, arrayListOf(Utf8("we23"), Utf8("54")))
//    }
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
//    "generate array for a Set of strings" {
//
//      @Serializable
//      data class Test(val set: Set<String>)
//
//      val schema = Avro.default.schema(Test.serializer())
//      val record = Avro.default.toRecord(Test.serializer(), Test(setOf("qwe", "dfsg")))
//      record shouldBe ImmutableRecord(
//          schema,
//          arrayListOf(Utf8("qwe"), Utf8("dfsg"))
//      )
//    }
//    "generate array for a Set of doubles" {
//
//      @Serializable
//      data class Test(val set: Set<Double>)
//
//      val schema = Avro.default.schema(Test.serializer())
//      val record = Avro.default.toRecord(Test.serializer(), Test(setOf(45.4, 26.26, 9214.6, 124.56)))
//      record shouldBe ImmutableRecord(
//          schema,
//          arrayListOf(45.4, 26.26, 9214.6, 124.56)
//      )
//    }
  }

})

