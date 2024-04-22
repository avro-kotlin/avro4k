package com.github.avrokotlin.avro4k.encoding

import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.record
import io.kotest.core.spec.style.StringSpec
import kotlinx.serialization.Serializable

class InlineClassEncodingTest : StringSpec({
    "encode/decode @AvroInline" {
        AvroAssertions.assertThat(Product("123", Name("sneakers")))
            .isEncodedAs(record("123", "sneakers"))
    }
    "encode/decode @AvroInline at root" {
        AvroAssertions.assertThat(ValueClass(NestedValue("sneakers")))
            .isEncodedAs(record("sneakers"))
    }
}) {
    @Serializable
    @JvmInline
    private value class ValueClass(val value: NestedValue)

    @Serializable
    private data class NestedValue(val field: String)

    @Serializable
    @JvmInline
    private value class Name(val value: String)

    @Serializable
    private data class Product(val id: String, val name: Name)
}