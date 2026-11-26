package com.github.avrokotlin.avro4k.encoding

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.encodeToBytesUsingApacheLib
import com.github.avrokotlin.avro4k.serializer.InstantSerializer
import com.github.avrokotlin.avro4k.serializer.InstantToMicroSerializer
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import kotlinx.serialization.SerializationException
import org.apache.avro.LogicalTypes
import org.apache.avro.SchemaBuilder
import java.time.Instant

internal class InstantEncodingTest : StringSpec({
    // 2020-01-01T12:34:56.000424123Z
    val instant = Instant.ofEpochSecond(1577882096, 424_123)
    val instantNanos = 1577882096000424123L

    // 1969-12-31T23:59:59.5Z
    val preEpochInstant = Instant.ofEpochSecond(-1, 500_000_000)
    val preEpochNanos = -500_000_000L

    val timestampNanos = LogicalTypes.timestampNanos().addToSchema(SchemaBuilder.builder().longType())

    listOf(InstantSerializer, InstantToMicroSerializer).forEach { serializer ->
        "${serializer::class.simpleName} encodes and decodes timestamp-nanos" {
            Avro.encodeToByteArray(timestampNanos, serializer, instant) shouldBe encodeToBytesUsingApacheLib(timestampNanos, instantNanos)
            Avro.decodeFromByteArray(timestampNanos, serializer, encodeToBytesUsingApacheLib(timestampNanos, instantNanos)) shouldBe instant
        }

        "${serializer::class.simpleName} encodes and decodes pre-epoch fractional timestamp-nanos" {
            Avro.encodeToByteArray(timestampNanos, serializer, preEpochInstant) shouldBe encodeToBytesUsingApacheLib(timestampNanos, preEpochNanos)
            Avro.decodeFromByteArray(timestampNanos, serializer, encodeToBytesUsingApacheLib(timestampNanos, preEpochNanos)) shouldBe preEpochInstant
        }
    }

    "encode and decode Instant at both ends of the timestamp-nanos range" {
        listOf(Long.MIN_VALUE, Long.MAX_VALUE).forEach { nanos ->
            val boundary = Instant.EPOCH.plusNanos(nanos)

            Avro.encodeToByteArray(timestampNanos, InstantSerializer, boundary) shouldBe encodeToBytesUsingApacheLib(timestampNanos, nanos)
            Avro.decodeFromByteArray(timestampNanos, InstantSerializer, encodeToBytesUsingApacheLib(timestampNanos, nanos)) shouldBe boundary
        }
    }

    "reject Instant outside timestamp-nanos range" {
        listOf(Instant.parse("1677-09-21T00:00:00Z"), Instant.parse("2262-04-12T00:00:00Z")).forEach { outOfRangeInstant ->
            val error =
                shouldThrow<SerializationException> {
                    Avro.encodeToByteArray(timestampNanos, InstantSerializer, outOfRangeInstant)
                }

            error.message shouldContain "out of range for Avro timestamp-nanos"
        }
    }
})