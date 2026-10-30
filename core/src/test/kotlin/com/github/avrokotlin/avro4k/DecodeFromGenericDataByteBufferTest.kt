@file:Suppress("DEPRECATION")

package com.github.avrokotlin.avro4k

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.Schema
import java.nio.ByteBuffer

internal class DecodeFromGenericDataByteBufferTest : StringSpec({
    val bytesSchema = Schema.create(Schema.Type.BYTES)

    "decodes only the remaining bytes from a positioned heap buffer" {
        val buffer = ByteBuffer.wrap(byteArrayOf(9, 1, 2, 9)).apply {
            position(1)
            limit(3)
        }

        Avro.decodeFromGenericData<ByteArray>(bytesSchema, buffer).contentToString() shouldBe "[1, 2]"
        buffer.position() shouldBe 1
        buffer.limit() shouldBe 3
    }

    "decodes only the remaining bytes from a direct buffer" {
        val buffer = ByteBuffer.allocateDirect(4)
            .put(byteArrayOf(9, 1, 2, 9))
            .apply {
                position(1)
                limit(3)
            }

        Avro.decodeFromGenericData<ByteArray>(bytesSchema, buffer).contentToString() shouldBe "[1, 2]"
    }

    "decodes only the remaining bytes from a read-only buffer" {
        val buffer = ByteBuffer.wrap(byteArrayOf(9, 1, 2, 9)).apply {
            position(1)
            limit(3)
        }.asReadOnlyBuffer()

        Avro.decodeFromGenericData<ByteArray>(bytesSchema, buffer).contentToString() shouldBe "[1, 2]"
    }
})
