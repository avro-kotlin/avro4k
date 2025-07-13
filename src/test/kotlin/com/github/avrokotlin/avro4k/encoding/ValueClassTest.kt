package com.github.avrokotlin.avro4k.encoding

import com.github.avrokotlin.avro4k.AvroAssertions
import io.kotest.core.spec.style.StringSpec
import kotlinx.serialization.Serializable

class ValueClassTest : StringSpec({
    "encode/decode value classes" {
        AvroAssertions.assertThat(ValueClass(42))
            .isEncodedAs(42)
        AvroAssertions.assertThat<ValueClassNullable?>(null)
            .isEncodedAs(null)
        AvroAssertions.assertThat<ValueClassNullable?>(ValueClassNullable(42))
            .isEncodedAs(42)
        AvroAssertions.assertThat<ValueClassNullable?>(ValueClassNullable(null))
            .isEncodedAs(null, expectedDecodedValue = null)
    }
}) {
    @JvmInline
    @Serializable
    private value class ValueClass(val value: Int)

    @JvmInline
    @Serializable
    private value class ValueClassNullable(val value: Int?)
}