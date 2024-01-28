@file:UseSerializers(BigDecimalSerializer::class)

package com.github.avrokotlin.avro4k.endecode

import com.github.avrokotlin.avro4k.record
import com.github.avrokotlin.avro4k.serializer.BigDecimalSerializer
import io.kotest.core.factory.TestFactory
import io.kotest.core.spec.style.FunSpec
import io.kotest.core.spec.style.stringSpec
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import org.apache.avro.Conversions
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import java.math.BigDecimal

class BigDecimalEncoderTest : FunSpec({
    includeForEveryEncoder { bigDecimalEncoderTests(it) }
})

fun bigDecimalEncoderTests(encoderToTest: EnDecoder): TestFactory {
    @Serializable
    data class BigDecimalTest(val decimal: BigDecimal)

    return stringSpec {
        "use byte array for BigDecimal" {
            val schema = encoderToTest.avro.schema(BigDecimalTest.serializer())
            val obj = BigDecimalTest(BigDecimal("12.34"))
            val s = schema.getField("decimal").schema()
            val bytes = Conversions.DecimalConversion().toBytes(obj.decimal, s, s.logicalType)

            encoderToTest.testEncodeDecode(value = obj, shouldMatch = record(bytes), schema = schema)
        }

        "allow BigDecimal to be en-/decoded as strings" {
            val decimalSchema = Schema.create(Schema.Type.STRING)
            val schema =
                SchemaBuilder.record("Test").fields()
                    .name("decimal").type(decimalSchema).noDefault()
                    .endRecord()
            encoderToTest.testEncodeDecode(
                value = BigDecimalTest(BigDecimal("123.456")),
                shouldMatch = record("123.456"),
                schema = schema
            )
        }
        "support nullable big decimals" {
            @Serializable
            data class NullableBigDecimalTest(val big: BigDecimal?)

            val schema = encoderToTest.avro.schema(NullableBigDecimalTest.serializer())

            val obj = NullableBigDecimalTest(BigDecimal("123.40"))
            val bigSchema = schema.getField("big").schema().types[1] // Nullable is encoded as Union
            val bytes = Conversions.DecimalConversion().toBytes(obj.big, bigSchema, bigSchema.logicalType)
            encoderToTest.testEncodeDecode(obj, record(bytes))
            encoderToTest.testEncodeDecode(NullableBigDecimalTest(null), record(null))
        }

        "allow BigDecimal to be en-/decoded as generic fixed" {
            // Schema needs to have the precision of 16 in order to serialize a 8 digit integer with a scale of 8
            val decimal = LogicalTypes.decimal(16, 8).addToSchema(Schema.createFixed("decimal", null, null, 8))

            val schema =
                SchemaBuilder.record("Test").fields()
                    .name("decimal").type(decimal).noDefault()
                    .endRecord()
            encoderToTest.testEncodeDecode(
                value = BigDecimalTest(BigDecimal("12345678.00000000")),
                shouldMatch = record(GenericData.Fixed(decimal, byteArrayOf(0, 4, 98, -43, 55, 43, -114, 0))),
                schema = schema
            )
        }
    }
}