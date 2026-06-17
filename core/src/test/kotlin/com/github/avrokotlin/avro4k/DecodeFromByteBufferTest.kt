package com.github.avrokotlin.avro4k

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToByteArray
import java.nio.ByteBuffer

internal class DecodeFromByteBufferTest : StringSpec({
    val value = SimpleRecord(name = "alice", age = 42)
    val encoded = Avro.encodeToByteArray(value)

    "decodes from ByteBuffer starting at position 0" {
        val buffer = ByteBuffer.wrap(encoded)

        val decoded = Avro.decodeFromByteBuffer<SimpleRecord>(buffer)

        decoded shouldBe value
    }

    "decodes from ByteBuffer with a prefix (non-zero initial position)" {
        // Simulates a Confluent Schema Registry payload: magic byte (0x00) + 4-byte schema id
        val prefix = byteArrayOf(0x00, 0x00, 0x00, 0x00, 42)
        val buffer = ByteBuffer.wrap(prefix + encoded)
        buffer.position(prefix.size)

        val decoded = Avro.decodeFromByteBuffer<SimpleRecord>(buffer)

        decoded shouldBe value
    }

    "does not throw when there are remaining bytes after the record" {
        val suffix = byteArrayOf(0x01, 0x02, 0x03)
        val buffer = ByteBuffer.wrap(encoded + suffix)

        val decoded = Avro.decodeFromByteBuffer<SimpleRecord>(buffer)

        decoded shouldBe value
    }

    "advances the ByteBuffer position past the consumed bytes" {
        val suffix = byteArrayOf(0x01, 0x02, 0x03)
        val buffer = ByteBuffer.wrap(encoded + suffix)

        Avro.decodeFromByteBuffer<SimpleRecord>(buffer)

        buffer.position() shouldBe encoded.size
    }

    "advances the ByteBuffer position correctly when starting from a non-zero position" {
        val prefix = byteArrayOf(0x00, 0x00, 0x00, 0x00, 42)
        val suffix = byteArrayOf(0x01, 0x02, 0x03)
        val buffer = ByteBuffer.wrap(prefix + encoded + suffix)
        buffer.position(prefix.size)

        Avro.decodeFromByteBuffer<SimpleRecord>(buffer)

        buffer.position() shouldBe prefix.size + encoded.size
    }
}) {
    @Serializable
    private data class SimpleRecord(val name: String, val age: Int)
}