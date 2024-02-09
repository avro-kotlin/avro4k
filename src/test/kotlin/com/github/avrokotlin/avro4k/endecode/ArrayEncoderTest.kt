package com.github.avrokotlin.avro4k.endecode

import com.github.avrokotlin.avro4k.record
import io.kotest.core.factory.TestFactory
import io.kotest.core.spec.style.WordSpec
import io.kotest.core.spec.style.wordSpec
import kotlinx.serialization.Serializable

class ArrayEncoderTest : WordSpec({
    includeForEveryEncoder { arrayEncodingTests(it) }
})

@Suppress("ArrayInDataClass")
fun arrayEncodingTests(encoderToTest: EnDecoder): TestFactory {
    return wordSpec {
        "en-/decoder" should {
            "generate GenericData.Array for an Array<Boolean>" {
                @Serializable
                data class ArrayBooleanTest(val a: Array<Boolean>) {
                    override fun equals(other: Any?): Boolean {
                        val o = other as? ArrayBooleanTest
                        return o?.a?.contentEquals(o.a) == true
                    }

                    override fun hashCode(): Int {
                        return a.contentHashCode()
                    }
                }

                val value = ArrayBooleanTest(arrayOf(true, false, true))
                encoderToTest.testEncodeDecode(value, record(value.a.asList()))
            }
            "support GenericData.Array for an Array<Boolean> with other fields" {
                @Serializable
                data class ArrayBooleanWithOthersTest(val a: String, val b: Array<Boolean>, val c: Long) {
                    override fun equals(other: Any?): Boolean {
                        if (this === other) return true
                        if (javaClass != other?.javaClass) return false

                        other as ArrayBooleanWithOthersTest

                        if (a != other.a) return false
                        if (!b.contentEquals(other.b)) return false
                        if (c != other.c) return false

                        return true
                    }

                    override fun hashCode(): Int {
                        var result = a.hashCode()
                        result = 31 * result + b.contentHashCode()
                        result = 31 * result + c.hashCode()
                        return result
                    }
                }

                val value = ArrayBooleanWithOthersTest("foo", arrayOf(true, false, true), 123L)
                encoderToTest.testEncodeDecode(
                    value,
                    record(
                        "foo",
                        listOf(true, false, true),
                        123L
                    )
                )
            }

            "generate GenericData.Array for a List<String>" {
                @Serializable
                data class ListStringTest(val a: List<String>)
                encoderToTest.testEncodeDecode(
                    ListStringTest(listOf("we23", "54z")),
                    record(
                        listOf("we23", "54z")
                    )
                )
            }
            "generate GenericData.Array for a Set<Long>" {
                @Serializable
                data class SetLongTest(val a: Set<Long>)

                val value = SetLongTest(setOf(123L, 643L, 912L))
                val record = record(listOf(123L, 643L, 912))
                encoderToTest.testEncodeDecode(value, record)
            }
        }
    }
}