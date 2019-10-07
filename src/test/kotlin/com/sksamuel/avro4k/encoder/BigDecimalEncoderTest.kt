@file:UseSerializers(BigDecimalSerializer::class)

package com.sksamuel.avro4k.encoder

import com.sksamuel.avro4k.Avro
import com.sksamuel.avro4k.ListRecord
import com.sksamuel.avro4k.serializer.BigDecimalSerializer
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import org.apache.avro.Conversions
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericFixed
import org.apache.avro.util.Utf8
import java.math.BigDecimal

class BigDecimalEncoderTest : FunSpec({

   test("use byte array for decimal") {

      @Serializable
      data class Test(val decimal: BigDecimal)

      val schema = Avro.default.schema(Test.serializer())

      val obj = Test(BigDecimal("12.34"))
      val s = schema.getField("decimal").schema()
      val bytes = Conversions.DecimalConversion().toBytes(BigDecimal("12.34"), s, s.logicalType)

      Avro.default.toRecord(Test.serializer(), schema, obj) shouldBe ListRecord(schema, bytes)
   }

   test("allow decimals to be encoded as strings") {

      @Serializable
      data class Test(val decimal: BigDecimal)

      val schema = SchemaBuilder.record("Test").fields()
         .name("decimal").type(Schema.create(Schema.Type.STRING)).noDefault()
         .endRecord()

      Avro.default.toRecord(Test.serializer(), schema, Test(BigDecimal("123.456"))) shouldBe
         ListRecord(schema, Utf8("123.456"))
   }

//   test("Allow Override of roundingMode") {
//
//      data class Test(@ScalePrecision(2, 10) val decimal: BigDecimal)
//
//      implicit val sp = 
//      val schema = AvroSchema[Test]
//      val s = schema.getField("decimal").schema()
//
//      implicit val roundingMode = RoundingMode.HALF_UP
//
//      val bytesRoundedDown = Conversions.DecimalConversion().toBytes(BigDecimal(12.34).bigDecimal, s, s.getLogicalType)
//      Encoder[Test].encode(Test(12.3449), schema, DefaultFieldMapper) shouldBe ImmutableRecord(schema, Vector(bytesRoundedDown))
//
//      val bytesRoundedUp = Conversions.DecimalConversion().toBytes(BigDecimal(12.35).bigDecimal, s, s.getLogicalType)
//      Encoder[Test].encode(Test(12.345), schema, DefaultFieldMapper) shouldBe ImmutableRecord(schema, Vector(bytesRoundedUp))
//   }

   test("support nullable big decimals") {

      @Serializable
      data class Test(val big: BigDecimal?)

      val schema = Avro.default.schema(Test.serializer())

      val s = schema.getField("big").schema().types.first { it.type != Schema.Type.NULL }
      val bytes = Conversions.DecimalConversion().toBytes(BigDecimal("123.4").setScale(2), s, s.logicalType)

      Avro.default.toRecord(Test.serializer(), schema, Test(BigDecimal("123.4"))) shouldBe ListRecord(schema, bytes)
      Avro.default.toRecord(Test.serializer(), schema, Test(null)) shouldBe ListRecord(schema, null)
   }

   test("allow bigdecimals to be encoded as generic fixed") {

      @Serializable
      data class Test(val big: BigDecimal)

      val decimal = LogicalTypes.decimal(10, 8).addToSchema(Schema.createFixed("big", null, null, 8))

      val schema = SchemaBuilder.record("Test").fields()
         .name("big").type(decimal).noDefault()
         .endRecord()

      val big = Avro.default.toRecord(Test.serializer(), schema, Test(BigDecimal("12345678"))).get("big") as GenericFixed
      big.bytes() shouldBe byteArrayOf(0, 4, 98, -43, 55, 43, -114, 0)
   }
})