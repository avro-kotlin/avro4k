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
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

internal class LocalDateTimeEncodingTest : StringSpec({
    // 2020-01-01T12:34:56.000424
    val dateTime = LocalDateTime.of(2020, 1, 1, 12, 34, 56, 424_000)
    val dateTimeMillis = 1577882096000L
    val dateTimeMicros = 1577882096000424L

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