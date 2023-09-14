@file:UseSerializers(BigIntegerSerializer::class)

package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.serializer.BigIntegerSerializer
import io.kotest.core.factory.TestFactory
import io.kotest.core.spec.style.FunSpec
import io.kotest.core.spec.style.funSpec
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import java.math.BigInteger

fun bigIntegerEncoderTests(encoderToTest: EncoderToTest): TestFactory {
    return funSpec {
        test("use string for bigint") {
            @Serializable
            data class BigIntegerTest(val b: BigInteger)

            val test = BigIntegerTest(BigInteger("123123123123213213213123214325365477686789676234"))
            encoderToTest.testEncodeDecode(test, record("123123123123213213213123214325365477686789676234"))
        }

        test("encode nullable big ints") {
            @Serializable
            data class NullableBigIntegerTest(val b: BigInteger?)
            encoderToTest.testEncodeDecode(
                NullableBigIntegerTest(BigInteger("12312312312321312365477686789676234")),
                record("12312312312321312365477686789676234")
            )
            encoderToTest.testEncodeDecode(NullableBigIntegerTest(null), record(null))

        }
    }
}

class BigIntegerEncoderTest : FunSpec({
    includeForEveryEncoder { bigIntegerEncoderTests(it) }
})
