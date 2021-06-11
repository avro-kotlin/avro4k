@file:UseSerializers(BigDecimalSerializer::class)

package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.ListRecord
import com.github.avrokotlin.avro4k.serializer.BigDecimalSerializer
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.FunSpec
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

      val schema = Avro.default.schema(BigDecimalTest.serializer())

      val obj = BigDecimalTest(BigDecimal("12.34"))
      val s = schema.getField("decimal").schema()
      val bytes = Conversions.DecimalConversion().toBytes(BigDecimal("12.34"), s, s.logicalType)

      Avro.default.toRecord(BigDecimalTest.serializer(), schema, obj) shouldBe ListRecord(schema, bytes)
   }

   test("allow decimals to be encoded as strings") {

      val schema = SchemaBuilder.record("Test").fields()
         .name("decimal").type(Schema.create(Schema.Type.STRING)).noDefault()
         .endRecord()

      Avro.default.toRecord(BigDecimalTest.serializer(), schema, BigDecimalTest(BigDecimal("123.456"))) shouldBe
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

      val schema = Avro.default.schema(NullableBigDecimalTest.serializer())

      val s = schema.getField("big").schema().types.first { it.type != Schema.Type.NULL }
      val bytes = Conversions.DecimalConversion().toBytes(BigDecimal("123.4").setScale(2), s, s.logicalType)

      Avro.default.toRecord(NullableBigDecimalTest.serializer(), schema, NullableBigDecimalTest(BigDecimal("123.4"))) shouldBe ListRecord(schema, bytes)
      Avro.default.toRecord(NullableBigDecimalTest.serializer(), schema, NullableBigDecimalTest(null)) shouldBe ListRecord(schema, null)
   }

   test("allow bigdecimals to be encoded as generic fixed") {


      //Schema needs to have the precision of 16 in order to serialize a 8 digit integer with a scale of 8
      val decimal = LogicalTypes.decimal(16, 8).addToSchema(Schema.createFixed("decimal", null, null, 8))

      val schema = SchemaBuilder.record("Test").fields()
         .name("decimal").type(decimal).noDefault()
         .endRecord()

      val big = Avro.default.toRecord(BigDecimalTest.serializer(), schema, BigDecimalTest(BigDecimal("12345678"))).get("decimal") as GenericFixed
      big.bytes() shouldBe byteArrayOf(0, 4, 98, -43, 55, 43, -114, 0)
   }
}) {
   @Serializable
   data class BigDecimalTest(val decimal: BigDecimal)

   @Serializable
   data class NullableBigDecimalTest(val big: BigDecimal?)
}
