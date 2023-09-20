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
@Serializable
@JvmInline
value class Name(val value: String)
fun inlineEncodingTests(encoderToTest: EnDecoder): TestFactory {
    return stringSpec {
        "encode/decode @AvroInline" {
            @Serializable
            data class Product(val id: String, val name: Name)
            encoderToTest.testEncodeDecode(
                Product("123", Name("sneakers")),
                record("123", "sneakers")
            )
        }
    }
}

