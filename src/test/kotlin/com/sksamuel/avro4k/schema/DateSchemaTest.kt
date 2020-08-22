@file:UseSerializers(
   LocalDateSerializer::class,
   LocalTimeSerializer::class,
   TimestampSerializer::class,
   InstantSerializer::class,
   LocalDateTimeSerializer::class
)

package com.sksamuel.avro4k.schema

import com.sksamuel.avro4k.Avro
import com.sksamuel.avro4k.serializer.*
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime

class DateSchemaTest : FunSpec({

   test("generate date logical type for LocalDate") {

      @Serializable
      data class LocalDateTest(val date: LocalDate)

      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/localdate.json"))
      val schema = Avro.default.schema(LocalDateTest.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }

   test("generate date logical type for nullable LocalDate") {

      @Serializable
      data class LocalDateTest(val date: LocalDate?)

      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/localdate_nullable.json"))
      val schema = Avro.default.schema(LocalDateTest.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }

   test("generate time logical type for LocalTime") {

      @Serializable
      data class LocalTimeTest(val time: LocalTime)

      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/localtime.json"))
      val schema = Avro.default.schema(LocalTimeTest.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }

   test("generate time logical type for LocalDateTime") {

      @Serializable
      data class LocalTimeTest(val time: LocalDateTime)

      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/localdatetime.json"))
      val schema = Avro.default.schema(LocalTimeTest.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }

   test("generate timestamp-millis logical type for Instant") {

      @Serializable
      data class InstantTest(val instant: Instant)

      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/instant.json"))
      val schema = Avro.default.schema(InstantTest.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }

   test("generate timestamp-millis logical type for Timestamp") {

      @Serializable
      data class TimestampTest(val ts: Timestamp)

      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/timestamp.json"))
      val schema = Avro.default.schema(TimestampTest.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }

   test("generate timestamp-millis logical type for nullable Timestamp") {

      @Serializable
      data class Test(val ts: Timestamp?)

      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/timestamp_nullable.json"))
      val schema = Avro.default.schema(Test.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }
})