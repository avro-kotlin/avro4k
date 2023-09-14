package com.github.avrokotlin.avro4k.encoder

import io.kotest.core.factory.TestFactory
import io.kotest.core.spec.style.FunSpec
import io.kotest.core.spec.style.funSpec
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient

fun transientEncoderTests(encoderToTest: EncoderToTest): TestFactory {
    @Serializable
    data class Foo(val a: String, @Transient val b: String = "foo", val c: String)
    return funSpec {

        test("encoder should skip @Transient fields") {
            val value = Foo("a", "b", "c")
            encoderToTest.testEncodeIsEqual(value, record("a", "c"))
        }
    }
}

class TransientEncoderTest : FunSpec({
    includeForEveryEncoder { transientEncoderTests(it) }
}) 
