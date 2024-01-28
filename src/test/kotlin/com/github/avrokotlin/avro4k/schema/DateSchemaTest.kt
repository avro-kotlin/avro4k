@file:UseSerializers(
    LocalDateSerializer::class,
    LocalTimeSerializer::class,
    TimestampSerializer::class,
    InstantSerializer::class,
    LocalDateTimeSerializer::class
)

package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.serializer.InstantSerializer
import com.github.avrokotlin.avro4k.serializer.LocalDateSerializer
import com.github.avrokotlin.avro4k.serializer.LocalDateTimeSerializer
import com.github.avrokotlin.avro4k.serializer.LocalTimeSerializer
import com.github.avrokotlin.avro4k.serializer.TimestampSerializer
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime

class DateSchemaTest : FunSpec({

    test("generate date logical type for LocalDate") {

        val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/localdate.json"))
        val schema = Avro.default.schema(LocalDateTest.serializer())
        schema.toString(true) shouldBe expected.toString(true)
    }

    test("generate date logical type for nullable LocalDate") {

        val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/localdate_nullable.json"))
        val schema = Avro.default.schema(NullableLocalDateTest.serializer())
        schema.toString(true) shouldBe expected.toString(true)
    }

    test("generate time logical type for LocalTime") {

        val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/localtime.json"))
        val schema = Avro.default.schema(LocalTimeTest.serializer())
        schema.toString(true) shouldBe expected.toString(true)
    }

    test("generate time logical type for LocalDateTime") {

        val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/localdatetime.json"))
        val schema = Avro.default.schema(LocalDateTimeTest.serializer())
        schema.toString(true) shouldBe expected.toString(true)
    }

    test("generate timestamp-millis logical type for Instant") {

        val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/instant.json"))
        val schema = Avro.default.schema(InstantTest.serializer())
        schema.toString(true) shouldBe expected.toString(true)
    }

    test("generate timestamp-millis logical type for Timestamp") {

        val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/timestamp.json"))
        val schema = Avro.default.schema(TimestampTest.serializer())
        schema.toString(true) shouldBe expected.toString(true)
    }

    test("generate timestamp-millis logical type for nullable Timestamp") {

        val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/timestamp_nullable.json"))
        val schema = Avro.default.schema(Test.serializer())
        schema.toString(true) shouldBe expected.toString(true)
    }
}) {
    @Serializable
    data class LocalDateTest(val date: LocalDate)

    @Serializable
    data class NullableLocalDateTest(val date: LocalDate?)

    @Serializable
    data class LocalTimeTest(val time: LocalTime)

    @Serializable
    data class LocalDateTimeTest(val time: LocalDateTime)

    @Serializable
    data class InstantTest(val instant: Instant)

    @Serializable
    data class TimestampTest(val ts: Timestamp)

    @Serializable
    data class Test(val ts: Timestamp?)
}