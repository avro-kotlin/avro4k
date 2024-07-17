package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.AvroDecimal
import com.github.avrokotlin.avro4k.AvroFixed
import com.github.avrokotlin.avro4k.internal.nullable
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.Contextual
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import java.math.BigDecimal

internal class BigDecimalSchemaTest : FunSpec({
    test("support BigDecimal logical types") {
        AvroAssertions.assertThat<BigDecimalTest>()
            .generatesSchema(LogicalTypes.decimal(8, 2).addToSchema(Schema.create(Schema.Type.BYTES)))
    }

    test("support BigDecimal logical types as fixed") {
        AvroAssertions.assertThat<BigDecimalFixedTest>()
            .generatesSchema(LogicalTypes.decimal(5, 3).addToSchema(Schema.createFixed("field", null, null, 5)))
    }

    test("support nullable BigDecimal logical types") {
        AvroAssertions.assertThat<BigDecimalNullableTest>()
            .generatesSchema(LogicalTypes.decimal(2, 1).addToSchema(Schema.create(Schema.Type.BYTES)).nullable)
    }
}) {
    @JvmInline
    @Serializable
    private value class BigDecimalTest(
        @AvroDecimal(scale = 2, precision = 8)
        @Contextual
        val bigDecimal: BigDecimal,
    )

    @JvmInline
    @Serializable
    @SerialName("BigDecimalFixedTest")
    private value class BigDecimalFixedTest(
        @AvroDecimal(scale = 3, precision = 5)
        @AvroFixed(5)
        @Contextual
        val field: BigDecimal,
    )

    @JvmInline
    @Serializable
    private value class BigDecimalNullableTest(
        @AvroDecimal(scale = 1, precision = 2)
        @Contextual
        val bigDecimal: BigDecimal?,
    )
}