@file:UseSerializers(
    LocalDateTimeSerializer::class,
    LocalDateSerializer::class,
    LocalTimeSerializer::class,
    TimestampSerializer::class
)

package com.github.avrokotlin.avro4k.endecode

import com.github.avrokotlin.avro4k.record
import com.github.avrokotlin.avro4k.serializer.InstantSerializer
import com.github.avrokotlin.avro4k.serializer.InstantToMicroSerializer
import com.github.avrokotlin.avro4k.serializer.LocalDateSerializer
import com.github.avrokotlin.avro4k.serializer.LocalDateTimeSerializer
import com.github.avrokotlin.avro4k.serializer.LocalTimeSerializer
import com.github.avrokotlin.avro4k.serializer.TimestampSerializer
import io.kotest.core.factory.TestFactory
import io.kotest.core.spec.style.FunSpec
import io.kotest.core.spec.style.stringSpec
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.temporal.ChronoUnit

class DateEncoderTest : FunSpec({
    includeForEveryEncoder { dateEncoderTests(it) }
})

fun dateEncoderTests(encoderToTest: EnDecoder): TestFactory {
    return stringSpec {
        "encode/decode LocalTime as an Int" {
            @Serializable
            data class LocalTimeTest(val t: LocalTime)
            encoderToTest.testEncodeDecode(LocalTimeTest(LocalTime.of(12, 50, 45)), record(46245000))
        }

        "encode/decode nullable LocalTime" {
            @Serializable
            data class NullableLocalTimeTest(val t: LocalTime?)
            encoderToTest.testEncodeDecode(NullableLocalTimeTest(LocalTime.of(12, 50, 45)), record(46245000))
            encoderToTest.testEncodeDecode(NullableLocalTimeTest(null), record(null))
        }

        "encode/decode LocalDate as an Int" {
            @Serializable
            data class LocalDateTest(val d: LocalDate)
            encoderToTest.testEncodeDecode(LocalDateTest(LocalDate.of(2018, 9, 10)), record(17784))
        }

        "encode/decode LocalDateTime as Long" {
            @Serializable
            data class LocalDateTimeTest(val dt: LocalDateTime)
            encoderToTest.testEncodeDecode(
                LocalDateTimeTest(LocalDateTime.of(2018, 9, 10, 11, 58, 59)),
                record(1536580739000L)
            )
        }

        "encode/decode Timestamp as Long" {
            @Serializable
            data class TimestampTest(val t: Timestamp)
            encoderToTest.testEncodeDecode(
                TimestampTest(Timestamp.from(Instant.ofEpochMilli(1538312231000L))),
                record(1538312231000L)
            )
        }

        "encode/decode nullable Timestamp as Long" {
            @Serializable
            data class NullableTimestampTest(val t: Timestamp?)
            encoderToTest.testEncodeDecode(NullableTimestampTest(null), record(null))
            encoderToTest.testEncodeDecode(
                NullableTimestampTest(Timestamp.from(Instant.ofEpochMilli(1538312231000L))),
                record(1538312231000L)
            )
        }

        "encode/decode Instant as Long" {
            @Serializable
            data class InstantMillisTest(
                @Serializable(with = InstantSerializer::class) val i: Instant,
            )
            encoderToTest.testEncodeDecode(
                InstantMillisTest(Instant.ofEpochMilli(1538312231000L)),
                record(1538312231000L)
            )
        }

        "encode/decode Instant with microseconds as Long" {
            @Serializable
            data class InstantMicrosTest(
                @Serializable(with = InstantToMicroSerializer::class) val i: Instant,
            )

            encoderToTest.testEncodeDecode(
                InstantMicrosTest(Instant.ofEpochMilli(1538312231000L).plus(5, ChronoUnit.MICROS)),
                record(1538312231000005L)
            )
        }
    }
}