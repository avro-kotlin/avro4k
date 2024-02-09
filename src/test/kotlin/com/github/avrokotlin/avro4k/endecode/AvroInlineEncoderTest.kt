package com.github.avrokotlin.avro4k.endecode

import com.github.avrokotlin.avro4k.record
import io.kotest.core.factory.TestFactory
import io.kotest.core.spec.style.FunSpec
import io.kotest.core.spec.style.stringSpec
import kotlinx.serialization.Serializable

class AvroInlineEncoderTest : FunSpec({
    includeForEveryEncoder {
        inlineEncodingTests(it)
    }
})

fun inlineEncodingTests(encoderToTest: EnDecoder): TestFactory {
    return stringSpec {
        "encode/decode @AvroInline" {
            encoderToTest.testEncodeDecode(
                Product("123", Name("sneakers")),
                record("123", "sneakers")
            )
        }
        "encode/decode @AvroInline at root" {
            encoderToTest.testEncodeDecode(
                ValueClass(NestedValue("sneakers")),
                record("sneakers")
            )
        }
    }
}

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