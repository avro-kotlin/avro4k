package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.AvroInline
import io.kotest.core.factory.TestFactory
import io.kotest.core.spec.style.FunSpec
import io.kotest.core.spec.style.wordSpec
import kotlinx.serialization.Serializable

fun inlineEncodingTests(encoderToTest: EncoderToTest): TestFactory {
    return wordSpec {
        "encoder" should {
            "encode @AvroInline" {
                @Serializable
                @AvroInline
                data class Name(val value: String)

                @Serializable
                data class Product(val id: String, val name: Name)
                encoderToTest.testEncodeDecode(
                    Product("123", Name("sneakers")),
                    record("123", "sneakers")
                )
            }
        }
    }
}

class AvroInlineEncoderTest : FunSpec({
    includeForEveryEncoder {
        inlineEncodingTests(it)
    }
})

