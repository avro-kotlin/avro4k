@file:UseSerializers(
   LocalDateTimeSerializer::class,
   LocalDateSerializer::class,
   LocalTimeSerializer::class,
   TimestampSerializer::class,
   InstantSerializer::class
)

package com.sksamuel.avro4k.decoder

import com.sksamuel.avro4k.Avro
import com.sksamuel.avro4k.serializer.*
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import org.apache.avro.generic.GenericData
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime

class DateDecoderTest : FunSpec({

   @Serializable
   data class WithLocalTime(val z: LocalTime)

   @Serializable
   data class WithLocalDate(val z: LocalDate)

   @Serializable
   data class WithLocalDateTime(val z: LocalDateTime)

   @Serializable
   data class WithTimestamp(val z: Timestamp)

   @Serializable
   data class WithInstant(val z: Instant)

   test("decode int to LocalTime") {
      val schema = Avro.default.schema(WithLocalTime.serializer())
      val record = GenericData.Record(schema)
      record.put("z", 46245000)
      Avro.default.fromRecord(WithLocalTime.serializer(), record) shouldBe
         WithLocalTime(LocalTime.of(12, 50, 45))
   }

   test("decode int to LocalDate") {
      val schema = Avro.default.schema(WithLocalDate.serializer())
      val record = GenericData.Record(schema)
      record.put("z", 17784)
      Avro.default.fromRecord(WithLocalDate.serializer(), record) shouldBe
         WithLocalDate(LocalDate.of(2018, 9, 10))
   }

   test("decode long to LocalDateTime") {
      val schema = Avro.default.schema(WithLocalDateTime.serializer())
      val record = GenericData.Record(schema)
      record.put("z", 1536580739000L)
      Avro.default.fromRecord(WithLocalDateTime.serializer(), record) shouldBe
         WithLocalDateTime(LocalDateTime.of(2018, 9, 10, 11, 58, 59))
   }

   test("decode long to Timestamp") {
      val schema = Avro.default.schema(WithTimestamp.serializer())
      val record = GenericData.Record(schema)
      record.put("z", 1538312231000L)
      Avro.default.fromRecord(WithTimestamp.serializer(), record) shouldBe
         WithTimestamp(Timestamp(1538312231000L))
   }

   test("decode long to Instant") {
      val schema = Avro.default.schema(WithInstant.serializer())
      val record = GenericData.Record(schema)
      record.put("z", 1538312231000L)
      Avro.default.fromRecord(WithInstant.serializer(), record) shouldBe
         WithInstant(Instant.ofEpochMilli(1538312231000L))
   }
})