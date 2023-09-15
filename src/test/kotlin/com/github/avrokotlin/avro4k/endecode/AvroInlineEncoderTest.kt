package com.github.avrokotlin.avro4k.endecode

import com.github.avrokotlin.avro4k.AvroInline
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

