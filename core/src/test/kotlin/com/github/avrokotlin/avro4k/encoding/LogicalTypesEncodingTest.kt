package com.github.avrokotlin.avro4k.encoding

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.AvroDecimal
import com.github.avrokotlin.avro4k.AvroFixed
import com.github.avrokotlin.avro4k.AvroStringable
import com.github.avrokotlin.avro4k.record
import com.github.avrokotlin.avro4k.schema
import com.github.avrokotlin.avro4k.serializer.InstantToMicroSerializer
import io.kotest.core.spec.style.StringSpec
import kotlinx.serialization.Contextual
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.apache.avro.Conversions
import org.apache.avro.SchemaBuilder
import java.math.BigDecimal
import java.math.BigInteger
import java.net.URL
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.util.UUID
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

internal class LogicalTypesEncodingTest : StringSpec({
    "support logical types at root level" {
        val schema = Avro.schema<LogicalTypes>().fields[0].schema()
        AvroAssertions.assertThat(BigDecimal("123.45"))
            .isEncodedAs(
                Conversions.DecimalConversion().toBytes(
                    BigDecimal("123.45"),
                    null,
                    org.apache.avro.LogicalTypes.decimal(8, 2)
                ),
                writerSchema = schema
            )
    }

    "support non-nullable logical types" {
        println(Avro.schema<LogicalTypes>())
        AvroAssertions.assertThat(
            LogicalTypes(
                BigDecimal("123.45"),
                BigDecimal("123.45"),
                BigDecimal("123.45"),
                LocalDate.ofEpochDay(18262),
                LocalTime.ofSecondOfDay(45296),
                Instant.ofEpochSecond(1577889296),
                Instant.ofEpochSecond(1577889296, 424000),
                UUID.fromString("123e4567-e89b-12d3-a456-426614174000"),
                UUID.fromString("123e4567-e89b-12d3-a456-426614174001"),
                UUID.fromString("123e4567-e89b-12d3-a456-426614174002"),
                URL("http://example.com"),
                BigInteger("1234567890"),
                LocalDateTime.ofEpochSecond(1577889296, 424000000, java.time.ZoneOffset.UTC),
                36.hours + 24456.seconds,
                java.time.Period.of(12, 3, 4),
                (36.hours + 24456.seconds).toJavaDuration()
            )
        )
            .isEncodedAs(
                record(
                    Conversions.DecimalConversion().toBytes(
                        BigDecimal("123.45"),
                        null,
                        org.apache.avro.LogicalTypes.decimal(8, 2)
                    ),
                    Conversions.DecimalConversion().toFixed(
                        BigDecimal("123.45"),
                        SchemaBuilder.fixed("decimalFixed").size(42),
                        org.apache.avro.LogicalTypes.decimal(8, 2)
                    ),
                    "123.45",
                    18262,
                    45296000,
                    1577889296000,
                    1577889296000424,
                    "123e4567-e89b-12d3-a456-426614174000",
                    Conversions.UUIDConversion().toFixed(
                        UUID.fromString("123e4567-e89b-12d3-a456-426614174001"),
                        SchemaBuilder.fixed("uuid").size(16),
                        org.apache.avro.LogicalTypes.uuid()
                    ),
                    "123e4567-e89b-12d3-a456-426614174002",
                    "http://example.com",
                    "1234567890",
                    1577889296424,
                    byteArrayOf(0, 0, 0, 0, 1, 0, 0, 0, 64, 89, 8, 4),
                    byteArrayOf(-109, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0),
                    byteArrayOf(0, 0, 0, 0, 1, 0, 0, 0, 64, 89, 8, 4)
                )
            )
    }

    "support nullable logical types" {
        AvroAssertions.assertThat(
            NullableLogicalTypes(
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            )
        )
            .isEncodedAs(
                record(
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null
                )
            )
        AvroAssertions.assertThat(
            NullableLogicalTypes(
                BigDecimal("123.45"),
                BigDecimal("123.45"),
                BigDecimal("123.45"),
                LocalDate.ofEpochDay(18262),
                LocalTime.ofSecondOfDay(45296),
                Instant.ofEpochSecond(1577889296),
                Instant.ofEpochSecond(1577889296, 424000),
                UUID.fromString("123e4567-e89b-12d3-a456-426614174000"),
                UUID.fromString("123e4567-e89b-12d3-a456-426614174001"),
                UUID.fromString("123e4567-e89b-12d3-a456-426614174002"),
                URL("http://example.com"),
                BigInteger("1234567890"),
                LocalDateTime.ofEpochSecond(1577889296, 424000000, java.time.ZoneOffset.UTC),
                36.hours + 24456.seconds,
                java.time.Period.of(12, 3, 4),
                (36.hours + 24456.seconds).toJavaDuration()
            )
        )
            .isEncodedAs(
                record(
                    Conversions.DecimalConversion().toBytes(
                        BigDecimal("123.45"),
                        null,
                        org.apache.avro.LogicalTypes.decimal(8, 2)
                    ),
                    Conversions.DecimalConversion().toFixed(
                        BigDecimal("123.45"),
                        SchemaBuilder.fixed("com.github.avrokotlin.avro4k.encoding.LogicalTypesEncodingTest.decimalFixedNullable").size(42),
                        org.apache.avro.LogicalTypes.decimal(8, 2)
                    ),
                    "123.45",
                    18262,
                    45296000,
                    1577889296000,
                    1577889296000424,
                    "123e4567-e89b-12d3-a456-426614174000",
                    Conversions.UUIDConversion().toFixed(
                        UUID.fromString("123e4567-e89b-12d3-a456-426614174001"),
                        SchemaBuilder.fixed("uuidFixed").namespace("com.github.avrokotlin.avro4k.encoding.LogicalTypesEncodingTest").size(16),
                        org.apache.avro.LogicalTypes.uuid()
                    ),
                    "123e4567-e89b-12d3-a456-426614174002",
                    "http://example.com",
                    "1234567890",
                    1577889296424,
                    byteArrayOf(0, 0, 0, 0, 1, 0, 0, 0, 64, 89, 8, 4),
                    byteArrayOf(-109, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0),
                    byteArrayOf(0, 0, 0, 0, 1, 0, 0, 0, 64, 89, 8, 4)
                )
            )
    }
}) {
    @Serializable
    @SerialName("LogicalTypes")
    private data class LogicalTypes(
        @Contextual @AvroDecimal(scale = 2, precision = 8) val decimalBytes: BigDecimal,
        @Contextual @AvroDecimal(scale = 2, precision = 8) @AvroFixed(42) val decimalFixed: BigDecimal,
        @Contextual @AvroStringable val decimalString: BigDecimal,
        @Contextual val date: LocalDate,
        @Contextual val time: LocalTime,
        @Contextual val instant: Instant,
        @Serializable(InstantToMicroSerializer::class) val instantMicros: Instant,
        @Contextual val uuidImplicit: UUID,
        @Contextual @AvroFixed(16) val uuidFixed: UUID,
        @Contextual @AvroStringable val uuidString: UUID,
        @Contextual val url: URL,
        @Contextual val bigInteger: BigInteger,
        @Contextual val dateTime: LocalDateTime,
        val kotlinDuration: kotlin.time.Duration,
        @Contextual val period: java.time.Period,
        @Contextual val javaDuration: java.time.Duration,
    )

    @Serializable
    private data class NullableLogicalTypes(
        @Contextual @AvroDecimal(scale = 2, precision = 8) val decimalBytesNullable: BigDecimal?,
        @Contextual @AvroDecimal(scale = 2, precision = 8) @AvroFixed(42) val decimalFixedNullable: BigDecimal?,
        @Contextual @AvroStringable val decimalStringNullable: BigDecimal?,
        @Contextual val dateNullable: LocalDate?,
        @Contextual val timeNullable: LocalTime?,
        @Contextual val instantNullable: Instant?,
        @Serializable(InstantToMicroSerializer::class) val instantMicrosNullable: Instant?,
        @Contextual val uuidImplicit: UUID?,
        @Contextual @AvroFixed(16) val uuidFixed: UUID?,
        @Contextual @AvroStringable val uuidString: UUID?,
        @Contextual val urlNullable: URL?,
        @Contextual val bigIntegerNullable: BigInteger?,
        @Contextual val dateTimeNullable: LocalDateTime?,
        val kotlinDuration: kotlin.time.Duration?,
        @Contextual val period: java.time.Period?,
        @Contextual val javaDuration: java.time.Duration?,
    )
}