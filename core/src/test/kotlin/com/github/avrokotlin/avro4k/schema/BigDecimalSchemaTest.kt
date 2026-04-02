package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.AvroDecimal
import com.github.avrokotlin.avro4k.AvroFixed
import com.github.avrokotlin.avro4k.internal.copy
import com.github.avrokotlin.avro4k.internal.nullable
import com.github.avrokotlin.avro4k.schema
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Contextual
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerializationException
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import java.math.BigDecimal

internal class BigDecimalSchemaTest : FunSpec({
    test("support BigDecimal decimal logical types") {
        AvroAssertions.assertThat<DecimalTest>()
            .generatesSchema(LogicalTypes.decimal(8, 2).addToSchema(Schema.create(Schema.Type.BYTES)))
    }

    test("support BigDecimal decimal logical types as fixed") {
        AvroAssertions.assertThat<DecimalFixedTest>()
            .generatesSchema(LogicalTypes.decimal(5, 3).addToSchema(Schema.createFixed("field", null, null, 5)))
    }

    test("BigDecimal as fixed without @AvroDecimal fails") {
        shouldThrow<SerializationException> {
            Avro.schema<DecimalFixedFailingTest>()
        }
    }

    test("support nullable BigDecimal decimal logical types") {
        AvroAssertions.assertThat<DecimalNullableTest>()
            .generatesSchema(LogicalTypes.decimal(2, 1).addToSchema(Schema.create(Schema.Type.BYTES)).nullable)
    }

    test("BigDecimal generates big-decimal logical type") {
        Avro.schema<BigDecimal>() shouldBe Schema.create(Schema.Type.BYTES).copy(logicalTypeName = "big-decimal")
        Avro.schema<BigDecimalTest>() shouldBe Schema.create(Schema.Type.BYTES).copy(logicalTypeName = "big-decimal")
    }
}) {
    @JvmInline
    @Serializable
    private value class BigDecimalTest(
        @Contextual
        val bigDecimal: BigDecimal,
    )

    @JvmInline
    @Serializable
    private value class DecimalTest(
        @AvroDecimal(scale = 2, precision = 8)
        @Contextual
        val bigDecimal: BigDecimal,
    )

    @JvmInline
    @Serializable
    @SerialName("DecimalFixedTest")
    private value class DecimalFixedTest(
        @AvroDecimal(scale = 3, precision = 5)
        @AvroFixed(5)
        @Contextual
        val field: BigDecimal,
    )

    @JvmInline
    @Serializable
    @SerialName("DecimalFixedTest")
    private value class DecimalFixedFailingTest(
        @AvroFixed(5)
        @Contextual
        val field: BigDecimal,
    )

    @JvmInline
    @Serializable
    private value class DecimalNullableTest(
        @AvroDecimal(scale = 1, precision = 2)
        @Contextual
        val bigDecimal: BigDecimal?,
    )
}