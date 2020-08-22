package com.sksamuel.avro4k.decoder

import com.sksamuel.avro4k.Avro
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import kotlinx.serialization.Serializable
import org.apache.avro.generic.GenericData

@Serializable
data class FooString(val str: String)

@Serializable
data class FooDouble(val d: Double)

@Serializable
data class FooBoolean(val b: Boolean)

@Serializable
data class FooFloat(val f: Float)

@Serializable
data class FooLong(val l: Long)

@Serializable
data class FooInt(val i: Int)

@Serializable
data class FooByte(val b: Byte)

class BasicDecoderTest : FunSpec({

   test("decode strings") {
      val schema = Avro.default.schema(FooString.serializer())
      val record = GenericData.Record(schema)
      record.put("str", "hello")
      Avro.default.fromRecord(FooString.serializer(), record) shouldBe FooString("hello")
   }
   test("decode longs") {
      val schema = Avro.default.schema(FooLong.serializer())
      val record = GenericData.Record(schema)
      record.put("l", 123456L)
      Avro.default.fromRecord(FooLong.serializer(), record) shouldBe FooLong(123456L)
   }
   test("decode doubles") {
      val schema = Avro.default.schema(FooDouble.serializer())
      val record = GenericData.Record(schema)
      record.put("d", 123.435)
      Avro.default.fromRecord(FooDouble.serializer(), record) shouldBe FooDouble(123.435)
   }
   test("decode booleans") {
      val schema = Avro.default.schema(FooBoolean.serializer())
      val record = GenericData.Record(schema)
      record.put("b", true)
      Avro.default.fromRecord(FooBoolean.serializer(), record) shouldBe FooBoolean(true)
   }
   test("decode floats") {
      val schema = Avro.default.schema(FooFloat.serializer())
      val record = GenericData.Record(schema)
      record.put("f", 123.435F)
      Avro.default.fromRecord(FooFloat.serializer(), record) shouldBe FooFloat(123.435F)
   }
   test("decode ints") {
      val schema = Avro.default.schema(FooInt.serializer())
      val record = GenericData.Record(schema)
      record.put("i", 123)
      Avro.default.fromRecord(FooInt.serializer(), record) shouldBe FooInt(123)
   }
   test("decode bytes") {
      val schema = Avro.default.schema(FooByte.serializer())
      val record = GenericData.Record(schema)
      record.put("b", 123.toByte())
      Avro.default.fromRecord(FooByte.serializer(), record) shouldBe FooByte(123)
   }
})

