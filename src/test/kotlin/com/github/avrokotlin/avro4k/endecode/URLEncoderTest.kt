@file:UseSerializers(URLSerializer::class)

package com.github.avrokotlin.avro4k.endecode

import com.github.avrokotlin.avro4k.record
import com.github.avrokotlin.avro4k.serializer.URLSerializer
import io.kotest.core.factory.TestFactory
import io.kotest.core.spec.style.FunSpec
import io.kotest.core.spec.style.stringSpec
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import java.net.URL

class URLEncoderTest : FunSpec({
    includeForEveryEncoder { urlEncoderTests(it) }
})
fun urlEncoderTests(enDecoder: EnDecoder<*>): TestFactory {
    return stringSpec {
        "encode/decode URLs as string" {
            @Serializable
            data class UrlTest(val b: URL)

            val test = UrlTest(URL("https://www.sksamuel.com"))
            enDecoder.testEncodeDecode(test, record("https://www.sksamuel.com"))
        }

        "encode/decode nullable URLs" {
            @Serializable
            data class NullableUrlTest(val b: URL?)
            enDecoder.testEncodeDecode(
                NullableUrlTest(URL("https://www.sksamuel.com")), record("https://www.sksamuel.com")
            )
            enDecoder.testEncodeDecode(NullableUrlTest(null), record(null))
        }
    }
}
