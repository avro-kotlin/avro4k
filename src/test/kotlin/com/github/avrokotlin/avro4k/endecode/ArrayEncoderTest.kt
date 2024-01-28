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
                data class ArrayBooleanTest(val a: Array<Boolean>)

                val value = ArrayBooleanTest(arrayOf(true, false, true))
                encoderToTest.testEncodeDecode(value, record(value.a.asList()))
            }
            "support GenericData.Array for an Array<Boolean> with other fields" {
                @Serializable
                data class ArrayBooleanWithOthersTest(val a: String, val b: Array<Boolean>, val c: Long)

                val value = ArrayBooleanWithOthersTest("foo", arrayOf(true, false, true), 123L)
                encoderToTest.testEncodeDecode(
                    value, record(
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
                    ListStringTest(listOf("we23", "54z")), record(
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