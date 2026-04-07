package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.internal.nullable
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.Contextual
import kotlinx.serialization.Serializable
import org.apache.avro.Schema
import java.math.BigInteger

internal class BigIntegerSchemaTest : FunSpec({
    test("support BigInteger as string") {
        AvroAssertions.assertThat<BigIntegerTest>()
            .generatesSchema(Schema.create(Schema.Type.STRING))
    }

    test("support nullable BigInteger as string") {
        AvroAssertions.assertThat<BigIntegerNullableTest>()
            .generatesSchema(Schema.create(Schema.Type.STRING).nullable)
    }
}) {
    @JvmInline
    @Serializable
    private value class BigIntegerTest(
        @Contextual val bigInteger: BigInteger,
    )

    @JvmInline
    @Serializable
    private value class BigIntegerNullableTest(
        @Contextual val bigInteger: BigInteger?,
    )
}