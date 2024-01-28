package com.github.avrokotlin.avro4k.endecode

import com.github.avrokotlin.avro4k.AvroName
import com.github.avrokotlin.avro4k.record
import io.kotest.core.factory.TestFactory
import io.kotest.core.spec.style.FunSpec
import io.kotest.core.spec.style.stringSpec
import kotlinx.serialization.Serializable

class AvroNameEncoderTest : FunSpec({
    includeForEveryEncoder { avroNameEncodingTests(it) }
})

fun avroNameEncodingTests(endecoder: EnDecoder): TestFactory {
    return stringSpec {
        "take into account @AvroName on fields" {
            @Serializable
            data class Foo(
                @AvroName("bar") val foo: String,
            )
            endecoder.testEncodeDecode(Foo("hello"), record("hello"))
        }
    }
}