package com.github.avrokotlin.avro4k.encoding

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.encodeToBytesUsingApacheLib
import com.github.avrokotlin.avro4k.record
import com.github.avrokotlin.avro4k.serializer.LocalDateTimeSerializer
import com.github.avrokotlin.avro4k.serializer.LocalDateTimeToLocalTimestampMillisSerializer
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerializationException
import kotlinx.serialization.builtins.nullable
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit

internal class LocalDateTimeEncodingTest : StringSpec({
    // 2020-01-01T12:34:56.000424
    val dateTime = LocalDateTime.of(2020, 1, 1, 12, 34, 56, 424_000)
    val dateTimeMillis = 1577882096000L
    val dateTimeMicros = 1577882096000424L

    // 2020-01-01T12:34:56.000424123
    val dateTimeWithNanos = LocalDateTime.of(2020, 1, 1, 12, 34, 56, 424_123)
    val dateTimeNanos = 1577882096000424123L

    // 1969-12-31T23:59:59.5
    val preEpochDateTime = LocalDateTime.of(1969, 12, 31, 23, 59, 59, 500_000_000)
    val preEpochNanos = -500_000_000L

    "encode LocalDateTime as local-timestamp-millis using writer schema" {
        val schema = LogicalTypes.localTimestampMillis().addToSchema(SchemaBuilder.builder().longType())

        val encoded = Avro.encodeToByteArray(schema, LocalDateTimeSerializer, dateTime)

        encoded shouldBe encodeToBytesUsingApacheLib(schema, dateTimeMillis)
    }

    "encode LocalDateTime as local-timestamp-micros using writer schema" {
        val schema = LogicalTypes.localTimestampMicros().addToSchema(SchemaBuilder.builder().longType())

        val encoded = Avro.encodeToByteArray(schema, LocalDateTimeSerializer, dateTime)

        encoded shouldBe encodeToBytesUsingApacheLib(schema, dateTimeMicros)
    }

    "encode nullable LocalDateTime as local-timestamp-millis using writer schema" {
        val schema = Schema.createUnion(Schema.create(Schema.Type.NULL), LogicalTypes.localTimestampMillis().addToSchema(SchemaBuilder.builder().longType()))

        val encoded = Avro.encodeToByteArray(schema, LocalDateTimeSerializer.nullable, dateTime)

        encoded shouldBe encodeToBytesUsingApacheLib(schema, dateTimeMillis)
    }

    "decode local-timestamp-millis to LocalDateTime" {
        val schema = LogicalTypes.localTimestampMillis().addToSchema(SchemaBuilder.builder().longType())

        val decoded = Avro.decodeFromByteArray(schema, LocalDateTimeSerializer, encodeToBytesUsingApacheLib(schema, dateTimeMillis))

        decoded shouldBe dateTime.truncatedTo(ChronoUnit.MILLIS)
    }

    "decode local-timestamp-micros to LocalDateTime" {
        val schema = LogicalTypes.localTimestampMicros().addToSchema(SchemaBuilder.builder().longType())

        val decoded = Avro.decodeFromByteArray(schema, LocalDateTimeSerializer, encodeToBytesUsingApacheLib(schema, dateTimeMicros))

        decoded shouldBe dateTime
    }

    "encode LocalDateTime as local-timestamp-nanos using writer schema" {
        val schema = LogicalTypes.localTimestampNanos().addToSchema(SchemaBuilder.builder().longType())

        val encoded = Avro.encodeToByteArray(schema, LocalDateTimeSerializer, dateTimeWithNanos)

        encoded shouldBe encodeToBytesUsingApacheLib(schema, dateTimeNanos)
    }

    "encode pre-epoch fractional LocalDateTime as local-timestamp-nanos" {
        val schema = LogicalTypes.localTimestampNanos().addToSchema(SchemaBuilder.builder().longType())

        val encoded = Avro.encodeToByteArray(schema, LocalDateTimeSerializer, preEpochDateTime)

        encoded shouldBe encodeToBytesUsingApacheLib(schema, preEpochNanos)
    }

    "decode local-timestamp-nanos to LocalDateTime" {
        val schema = LogicalTypes.localTimestampNanos().addToSchema(SchemaBuilder.builder().longType())

        val decoded = Avro.decodeFromByteArray(schema, LocalDateTimeSerializer, encodeToBytesUsingApacheLib(schema, dateTimeNanos))

        decoded shouldBe dateTimeWithNanos
    }

    "decode pre-epoch fractional local-timestamp-nanos to LocalDateTime" {
        val schema = LogicalTypes.localTimestampNanos().addToSchema(SchemaBuilder.builder().longType())

        val decoded = Avro.decodeFromByteArray(schema, LocalDateTimeSerializer, encodeToBytesUsingApacheLib(schema, preEpochNanos))

        decoded shouldBe preEpochDateTime
    }

    "encode and decode LocalDateTime at both ends of the local-timestamp-nanos range" {
        val schema = LogicalTypes.localTimestampNanos().addToSchema(SchemaBuilder.builder().longType())

        listOf(Long.MIN_VALUE, Long.MAX_VALUE).forEach { nanos ->
            val boundary = LocalDateTime.ofInstant(Instant.EPOCH.plus(nanos, ChronoUnit.NANOS), ZoneOffset.UTC)

            Avro.encodeToByteArray(schema, LocalDateTimeSerializer, boundary) shouldBe encodeToBytesUsingApacheLib(schema, nanos)
            Avro.decodeFromByteArray(schema, LocalDateTimeSerializer, encodeToBytesUsingApacheLib(schema, nanos)) shouldBe boundary
        }
    }

    "reject LocalDateTime outside local-timestamp-nanos range" {
        val schema = LogicalTypes.localTimestampNanos().addToSchema(SchemaBuilder.builder().longType())

        listOf(LocalDateTime.of(1677, 9, 21, 0, 0), LocalDateTime.of(2262, 4, 12, 0, 0)).forEach { outOfRangeDateTime ->
            val error =
                shouldThrow<SerializationException> {
                    Avro.encodeToByteArray(schema, LocalDateTimeSerializer, outOfRangeDateTime)
                }

            error.message shouldContain "out of range for Avro local-timestamp-nanos"
        }
    }

    "LocalDateTimeToLocalTimestampMillisSerializer writes local-timestamp-millis" {
        AvroAssertions.assertThat(LocalTimestampRecord(dateTime.truncatedTo(ChronoUnit.MILLIS)))
            .isEncodedAs(record(dateTimeMillis))
    }

    "decoding from an unsupported writer type names LocalDateTime" {
        val schema = SchemaBuilder.builder().booleanType()

        val error =
            shouldThrow<SerializationException> {
                Avro.decodeFromByteArray(schema, LocalDateTimeSerializer, encodeToBytesUsingApacheLib(schema, true))
            }

        error.message shouldContain "For LocalDateTime, expected type one of [LONG, STRING]"
    }
}) {
    @Serializable
    private data class LocalTimestampRecord(
        @Serializable(with = LocalDateTimeToLocalTimestampMillisSerializer::class)
        val dateTime: LocalDateTime,
    )
}