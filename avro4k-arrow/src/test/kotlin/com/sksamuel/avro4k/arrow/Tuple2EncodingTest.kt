package com.sksamuel.avro4k.arrow

import arrow.core.Tuple2
import com.sksamuel.avro4k.Avro
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import kotlinx.serialization.Serializable
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8

class Tuple2EncodingTest : FunSpec({

   test("encoding tuple2s") {
      @Serializable
      data class Test(@Serializable(Tuple2Serializer::class) val tuple2: Tuple2<Long, String>)

      val record = Avro.default.toRecord(Test.serializer(), Test(Tuple2(123L, "hello")))
      val tuple2 = record["tuple2"] as GenericRecord
      tuple2["a"] shouldBe 123L
      tuple2["b"] shouldBe Utf8("hello")
   }

   test("decoding tuple2s") {

      @Serializable
      data class Test(@Serializable(Tuple2Serializer::class) val tuple2: Tuple2<Long, String>)

      val schema = Avro.default.schema(Test.serializer())
      val tupleSchema = schema.fields[0].schema()

      val tuple = GenericData.Record(tupleSchema)
      tuple.put("a", 123L)
      tuple.put("b", Utf8("hello"))

      val record = GenericData.Record(schema)
      record.put("tuple2", tuple)

      Avro.default.fromRecord(Test.serializer(), record) shouldBe Test(Tuple2(123L, "hello"))
   }
})