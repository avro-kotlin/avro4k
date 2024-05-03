package com.github.avrokotlin.avro4k.encoding

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.AvroFixed
import com.github.avrokotlin.avro4k.record
import com.github.avrokotlin.avro4k.schema
import com.github.avrokotlin.avro4k.serializer.BigDecimalAsStringSerializer
import com.github.avrokotlin.avro4k.serializer.InstantToMicroSerializer
import io.kotest.core.spec.style.StringSpec
import kotlinx.serialization.Contextual
import kotlinx.serialization.Serializable
import org.apache.avro.Conversions
import org.apache.avro.SchemaBuilder
import java.math.BigDecimal
import java.math.BigInteger
import java.net.URL
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.util.UUID

class LogicalTypesEncodingTest : StringSpec({
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

    "should encode and decode logical types" {
        val logicalTypes =
            LogicalTypes(
                BigDecimal("123.45"),
                BigDecimal("123.45"),
                BigDecimal("123.45"),
                LocalDate.ofEpochDay(18262),
                LocalTime.ofSecondOfDay(45296),
                Timestamp(1577889296000),
                Instant.ofEpochSecond(1577889296),
                Instant.ofEpochSecond(1577889296, 424000),
                UUID.fromString("123e4567-e89b-12d3-a456-426614174000"),
                URL("http://example.com"),
                BigInteger("1234567890"),
                LocalDateTime.ofEpochSecond(1577889296, 424000000, java.time.ZoneOffset.UTC),
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

        AvroAssertions.assertThat(logicalTypes)
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
                    1577889296000,
                    1577889296000424,
                    "123e4567-e89b-12d3-a456-426614174000",
                    "http://example.com",
                    "1234567890",
                    1577889296424,
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
    }
}) {
    @Serializable
    data class LogicalTypes(
        @Contextual val decimalBytes: BigDecimal,
        @Contextual @AvroFixed(42) val decimalFixed: BigDecimal,
        @Serializable(BigDecimalAsStringSerializer::class) val decimalString: BigDecimal,
        @Contextual val date: LocalDate,
        @Contextual val time: LocalTime,
        @Contextual val timestamp: Timestamp,
        @Contextual val instant: Instant,
        @Serializable(InstantToMicroSerializer::class) val instantMicros: Instant,
        @Contextual val uuid: UUID,
        @Contextual val url: URL,
        @Contextual val bigInteger: BigInteger,
        @Contextual val dateTime: LocalDateTime,
        @Contextual val decimalBytesNullable: BigDecimal?,
        @Contextual @AvroFixed(42) val decimalFixedNullable: BigDecimal?,
        @Serializable(BigDecimalAsStringSerializer::class) val decimalStringNullable: BigDecimal?,
        @Contextual val dateNullable: LocalDate?,
        @Contextual val timeNullable: LocalTime?,
        @Contextual val timestampNullable: Timestamp?,
        @Contextual val instantNullable: Instant?,
        @Serializable(InstantToMicroSerializer::class) val instantMicrosNullable: Instant?,
        @Contextual val uuidNullable: UUID?,
        @Contextual val urlNullable: URL?,
        @Contextual val bigIntegerNullable: BigInteger?,
        @Contextual val dateTimeNullable: LocalDateTime?,
    )
}