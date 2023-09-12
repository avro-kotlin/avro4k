package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.ListRecord
import com.github.avrokotlin.avro4k.encodeToGenericData
import com.github.avrokotlin.avro4k.serializer.BigDecimalSerializer
import com.github.avrokotlin.avro4k.shouldBeContentOf
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable
import kotlinx.serialization.modules.serializersModuleOf
import org.apache.avro.Conversions
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.util.Utf8
import java.math.BigDecimal

class BigDecimalEncoderTest : FunSpec({

    test("encode field as bytes") {
        val schema = Avro.default.schema(BigDecimalTest.serializer())

        val value = BigDecimal("12.34")
        val s = schema.getField("decimal").schema()
        val bytes = Conversions.DecimalConversion().toBytes(value, s, s.logicalType)

        Avro.default.encodeToGenericData(BigDecimalTest(value), schema) shouldBeContentOf ListRecord(schema, bytes)
    }

    test("encode value as bytes") {
        val avro = Avro(serializersModuleOf(BigDecimalSerializer))

        val value = BigDecimal("12.34")
        val bytes = Conversions.DecimalConversion().toBytes(value, null, LogicalTypes.decimal(8, 2))

        avro.encodeToGenericData(value) shouldBe bytes
    }

    test("encode field as string") {
        val schema = SchemaBuilder.record("Test").fields()
                .name("decimal").type(Schema.create(Schema.Type.STRING)).noDefault()
                .endRecord()

        val valueString = "123.456"
        Avro.default.encodeToGenericData(BigDecimalTest(BigDecimal(valueString)), schema) shouldBeContentOf ListRecord(schema, valueString)
    }

    test("encode value as string") {
        val avro = Avro(serializersModuleOf(BigDecimalSerializer))

        val value = "12.34"

        avro.encodeToGenericData(BigDecimal(value), Schema.create(Schema.Type.STRING)) shouldBe Utf8(value)
    }

    test("encode null field") {
        val schema = Avro.default.schema(NullableBigDecimalTest.serializer())

        Avro.default.encodeToGenericData(NullableBigDecimalTest(null), schema) shouldBeContentOf ListRecord(schema, null)
    }

    test("encode field as fixed") {
        //Schema needs to have the precision of 16 in order to serialize a 8 digit integer with a scale of 8
        val decimal = LogicalTypes.decimal(16, 8).addToSchema(Schema.createFixed("decimal", null, null, 8))

        val schema = SchemaBuilder.record("Test").fields()
                .name("decimal").type(decimal).noDefault()
                .endRecord()

        val value = BigDecimal("12345678")
        val record = Avro.default.encodeToGenericData(BigDecimalTest(value), schema)
        val fixed = Conversions.DecimalConversion().toFixed(value, decimal, decimal.logicalType)

        record shouldBeContentOf ListRecord(schema, fixed)
    }
}) {
    @Serializable
    data class BigDecimalTest(@Serializable(with = BigDecimalSerializer::class) val decimal: BigDecimal)

    @Serializable
    data class NullableBigDecimalTest(@Serializable(with = BigDecimalSerializer::class) val big: BigDecimal?)
}
