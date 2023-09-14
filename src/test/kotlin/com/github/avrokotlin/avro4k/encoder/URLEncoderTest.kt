@file:UseSerializers(URLSerializer::class)

package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.serializer.URLSerializer
import io.kotest.core.factory.TestFactory
import io.kotest.core.spec.style.FunSpec
import io.kotest.core.spec.style.funSpec
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import java.net.URL

fun urlEncoderTests(encoderToTest: EncoderToTest): TestFactory {
    return funSpec {
        test("use string for URL") {
            @Serializable
            data class UrlTest(val b: URL)

            val test = UrlTest(URL("https://www.sksamuel.com"))
            encoderToTest.testEncodeDecode(test, record("https://www.sksamuel.com"))
        }

        test("encode nullable URLs") {
            @Serializable
            data class NullableUrlTest(val b: URL?)
            encoderToTest.testEncodeDecode(
                NullableUrlTest(URL("https://www.sksamuel.com")), record("https://www.sksamuel.com")
            )
            encoderToTest.testEncodeDecode(NullableUrlTest(null), record(null))
        }
    }
}

class URLEncoderTest : FunSpec({
    includeForEveryEncoder { urlEncoderTests(it) }
})
