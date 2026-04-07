package com.github.avrokotlin.avro4k

import com.github.avrokotlin.avro4k.internal.encoder.direct.KotlinxIoEncoder
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import kotlinx.io.Buffer
import kotlinx.io.readByteArray

class KotlinxIoEncoderTest : StringSpec() {
    init {
        "basic string is serialized correctly" {
            val buffer = Buffer()
            val string = "test"
            KotlinxIoEncoder(buffer).writeString(string)
            buffer.readByteArray() shouldBe byteArrayOf(4.zigZagByte()) + string.encodeToByteArray()
        }

        "special chars in string are serialized correctly" {
            val buffer = Buffer()
            val string = "àûTöç"
            KotlinxIoEncoder(buffer).writeString(string)
            buffer.readByteArray() shouldBe byteArrayOf(9.zigZagByte()) + string.encodeToByteArray()
        }
    }

    private fun Int.zigZagByte() = (this * 2).toByte()
}