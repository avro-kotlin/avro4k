@file:UseSerializers(
   LocalDateTimeSerializer::class,
   LocalDateSerializer::class,
   LocalTimeSerializer::class,
   TimestampSerializer::class,
)

package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.ListRecord
import com.github.avrokotlin.avro4k.encodeToGenericData
import com.github.avrokotlin.avro4k.serializer.InstantSerializer
import com.github.avrokotlin.avro4k.serializer.InstantToMicroSerializer
import com.github.avrokotlin.avro4k.serializer.LocalDateSerializer
import com.github.avrokotlin.avro4k.serializer.LocalDateTimeSerializer
import com.github.avrokotlin.avro4k.serializer.LocalTimeSerializer
import com.github.avrokotlin.avro4k.serializer.TimestampSerializer
import com.github.avrokotlin.avro4k.shouldBeContentOf
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.temporal.ChronoUnit

class DateEncoderTest : FunSpec({

   test("encode LocalTime as an Int") {

       val schema = Avro.default.schema(LocalTimeTest.serializer())
       Avro.default.encodeToGenericData(LocalTimeTest(LocalTime.of(12, 50, 45))) shouldBeContentOf
               ListRecord(schema, 46245000)
   }

   test("encode nullable LocalTime") {

       val schema = Avro.default.schema(NullableLocalTimeTest.serializer())
       Avro.default.encodeToGenericData(NullableLocalTimeTest(LocalTime.of(12, 50, 45))) shouldBeContentOf ListRecord(schema, 46245000)
       Avro.default.encodeToGenericData(NullableLocalTimeTest(null)) shouldBeContentOf ListRecord(schema, null)
   }

   test("encode LocalDate as an Int") {

       val schema = Avro.default.schema(LocalDateTest.serializer())
       Avro.default.encodeToGenericData(LocalDateTest(LocalDate.of(2018, 9, 10))) shouldBeContentOf
               ListRecord(schema, 17784)
   }

   test("encode LocalDateTime as Long") {

       val schema = Avro.default.schema(LocalDateTimeTest.serializer())
       Avro.default.encodeToGenericData(LocalDateTimeTest(LocalDateTime.of(2018, 9, 10, 11, 58, 59))) shouldBeContentOf
               ListRecord(schema, 1536580739000L)
   }

   test("encode Timestamp as Long") {

       val schema = Avro.default.schema(TimestampTest.serializer())
       Avro.default.encodeToGenericData(TimestampTest(Timestamp.from(Instant.ofEpochMilli(1538312231000L)))) shouldBeContentOf
               ListRecord(schema, 1538312231000L)
   }

   test("encode nullable Timestamp as Long") {

       val schema = Avro.default.schema(NullableTimestampTest.serializer())
       Avro.default.encodeToGenericData(NullableTimestampTest(null)) shouldBeContentOf ListRecord(schema, null)
       Avro.default.encodeToGenericData(NullableTimestampTest(Timestamp.from(Instant.ofEpochMilli(1538312231000L)))) shouldBeContentOf
               ListRecord(schema, 1538312231000L)
   }

   test("encode Instant as Long") {

       val schema = Avro.default.schema(InstantMillisTest.serializer())
       Avro.default.encodeToGenericData(InstantMillisTest(Instant.ofEpochMilli(1538312231000L))) shouldBeContentOf
               ListRecord(schema, 1538312231000L)
   }

   test("encode Instant with microseconds as Long") {

       val schema = Avro.default.schema(InstantMicrosTest.serializer())
       val time = Instant.ofEpochMilli(1538312231000L).plus(5, ChronoUnit.MICROS)

       Avro.default.encodeToGenericData(InstantMicrosTest(time)) shouldBeContentOf ListRecord(schema, 1538312231000005L)
   }
}) {
   @Serializable
   data class LocalTimeTest(val t: LocalTime)

   @Serializable
   data class NullableLocalTimeTest(val t: LocalTime?)

   @Serializable
   data class LocalDateTest(val d: LocalDate)

   @Serializable
   data class LocalDateTimeTest(val dt: LocalDateTime)

   @Serializable
   data class TimestampTest(val t: Timestamp)

   @Serializable
   data class NullableTimestampTest(val t: Timestamp?)

   @Serializable
   data class InstantMillisTest(@Serializable(with = InstantSerializer::class) val i: Instant)

   @Serializable
   data class InstantMicrosTest(@Serializable(with = InstantToMicroSerializer::class) val i: Instant)
}
