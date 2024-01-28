package com.github.avrokotlin.avro4k.endecode

import com.github.avrokotlin.avro4k.record
import io.kotest.core.factory.TestFactory
import io.kotest.core.spec.style.StringSpec
import io.kotest.core.spec.style.stringSpec
import kotlinx.serialization.SerialName
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

        "encode/decode a Map<string value class, String>" {
            @Serializable
            data class StringStringTest(val a: Map<MapStringKey, String>)
            enDecoder.testEncodeDecode(
                StringStringTest(mapOf(MapStringKey("a") to "x", MapStringKey("b") to "y", MapStringKey("c") to "z")),
                record(mapOf("a" to "x", "b" to "y", "c" to "z"))
            )
        }

        "encode/decode a Map<int value class, String>" {
            @Serializable
            data class StringStringTest(val a: Map<MapIntKey, String>)
            enDecoder.testEncodeDecode(
                StringStringTest(mapOf(MapIntKey(3) to "x", MapIntKey(1) to "y", MapIntKey(42) to "z")),
                record(mapOf("3" to "x", "1" to "y", "42" to "z"))
            )
        }

        "encode/decode a Map<enum, String>" {
            @Serializable
            data class StringStringTest(val a: Map<MyEnum, String>)
            enDecoder.testEncodeDecode(
                StringStringTest(mapOf(MyEnum.C to "x", MyEnum.A to "y", MyEnum.B to "z")),
                record(mapOf("z" to "x", "A" to "y", "B" to "z"))
            )
        }

        "encode/decode a Map<String, List<Byte>>" {
            @Serializable
            data class StringByteArrayTest(val a: Map<String, List<Byte>>)
            enDecoder.testEncodeDecode(
                StringByteArrayTest(
                    mapOf(
                        "a" to "x".toByteArray().toList(),
                        "b" to "y".toByteArray().toList(),
                        "c" to "z".toByteArray().toList()
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

@JvmInline
@Serializable
private value class MapStringKey(val value: String)

@JvmInline
@Serializable
private value class MapIntKey(val value: Int)

@Serializable
private enum class MyEnum {
    A,
    B,

    @SerialName("z")
    C,
}