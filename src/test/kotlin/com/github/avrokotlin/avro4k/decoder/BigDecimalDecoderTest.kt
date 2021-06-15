@file:UseSerializers(BigDecimalSerializer::class)

package com.github.avrokotlin.avro4k.decoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.serializer.BigDecimalSerializer
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import org.apache.avro.Conversions
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import java.math.BigDecimal

class BigDecimalDecoderTest : FunSpec({

   test("decode big decimals from bytes") {

      val logical = LogicalTypes.decimal(8, 2)

      val schema = SchemaBuilder.record("Test").fields()
         .name("b").type(logical.addToSchema(Schema.create(Schema.Type.BYTES))).noDefault()
         .endRecord()

      val bytes = Conversions.DecimalConversion().toBytes(BigDecimal("12.34"), schema.getField("b").schema(), logical)

      val record = GenericData.Record(schema)
      record.put("b", bytes)

      Avro.default.fromRecord(Test.serializer(), record) shouldBe Test(BigDecimal("12.34"))
   }

   test("decode big decimals from strings") {

      val schema = SchemaBuilder.record("Test").fields()
         .name("b").type(Schema.create(Schema.Type.STRING)).noDefault()
         .endRecord()

      val record = GenericData.Record(schema)
      record.put("b", Utf8("12.34"))

      Avro.default.fromRecord(Test.serializer(), record) shouldBe Test(BigDecimal("12.34"))
   }

   test("decode big decimals from fixed") {

      val logical = LogicalTypes.decimal(10, 8)

      val schema = SchemaBuilder.record("Test").fields()
         .name("b").type(logical.addToSchema(Schema.createFixed("b", null, null, 8))).noDefault()
         .endRecord()

      val record = GenericData.Record(schema)
      record.put("b", byteArrayOf(0, 4, 98, -43, 55, 43, -114, 0))

      Avro.default.fromRecord(Test.serializer(), record) shouldBe Test(BigDecimal("12345678.00000000"))
   }
}) {

   @Serializable
   data class Test(val b: BigDecimal)
}
