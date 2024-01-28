package com.github.avrokotlin.avro4k.endecode

import com.github.avrokotlin.avro4k.record
import io.kotest.core.factory.TestFactory
import io.kotest.core.spec.style.StringSpec
import io.kotest.core.spec.style.stringSpec
import kotlinx.serialization.Serializable
import java.nio.ByteBuffer

class MapEncoderTest : StringSpec({
    includeForEveryEncoder { mapEncoderTests(it) }
})

fun mapEncoderTests(enDecoder: EnDecoder): TestFactory {
    return stringSpec {
        "encode/decode a Map<String, Boolean>" {
            @Serializable
            data class StringBooleanTest(val a: Map<String, Boolean>)
            enDecoder.testEncodeDecode(
                StringBooleanTest(mapOf("a" to true, "b" to false, "c" to true)),
                record(mapOf("a" to true, "b" to false, "c" to true))
            )
        }

        "encode/decode a Map<String, String>" {
            @Serializable
            data class StringStringTest(val a: Map<String, String>)
            enDecoder.testEncodeDecode(
                StringStringTest(mapOf("a" to "x", "b" to "y", "c" to "z")),
                record(mapOf("a" to "x", "b" to "y", "c" to "z"))
            )
        }

        "encode/decode a Map<String, ByteArray>" {
            @Serializable
            data class StringByteArrayTest(val a: Map<String, ByteArray>)
            enDecoder.testEncodeDecode(
                StringByteArrayTest(
                    mapOf(
                        "a" to "x".toByteArray(),
                        "b" to "y".toByteArray(),
                        "c" to "z".toByteArray()
                    )
                ),
                record(
                    mapOf(
                        "a" to ByteBuffer.wrap("x".toByteArray()),
                        "b" to ByteBuffer.wrap("y".toByteArray()),
                        "c" to ByteBuffer.wrap("z".toByteArray())
                    )
                )
            )
        }

        "encode/decode a Map of records" {
            @Serializable
            data class Foo(val a: String, val b: Boolean)

            @Serializable
            data class StringFooTest(val a: Map<String, Foo>)
            enDecoder.testEncodeDecode(
                StringFooTest(mapOf("a" to Foo("x", true), "b" to Foo("y", false))),
                record(
                    mapOf(
                        "a" to record("x", true),
                        "b" to record("y", false)
                    )
                )
            )
        }
    }
}