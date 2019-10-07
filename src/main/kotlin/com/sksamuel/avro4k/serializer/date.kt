package com.sksamuel.avro4k.serializer

import kotlinx.serialization.Decoder
import kotlinx.serialization.SerialDescriptor
import kotlinx.serialization.SerializationException
import kotlinx.serialization.Serializer
import kotlinx.serialization.internal.IntDescriptor
import kotlinx.serialization.internal.StringDescriptor
import kotlinx.serialization.withName
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneOffset

@Serializer(forClass = LocalDate::class)
class LocalDateSerializer : AvroSerializer<LocalDate>() {

   companion object {
      const val name = "java.time.LocalDate"
   }

   override val descriptor: SerialDescriptor = StringDescriptor.withName(name)

   override fun toAvroValue(schema: Schema, value: LocalDate): Int = value.toEpochDay().toInt()
   override fun fromAvroValue(schema: Schema, decoder: Decoder): LocalDate = LocalDate.ofEpochDay(decoder.decodeLong())
}

@Serializer(forClass = LocalTime::class)
class LocalTimeSerializer : AvroSerializer<LocalTime>() {

   companion object {
      const val name = "java.time.LocalTime"
   }

   override val descriptor: SerialDescriptor = IntDescriptor.withName(name)

   override fun toAvroValue(schema: Schema, value: LocalTime): Int {
      return value.toSecondOfDay() * 1000 + value.nano / 1000
   }

   override fun fromAvroValue(schema: Schema, decoder: Decoder): LocalTime {
      // val schema = (encoder as FieldEncoder).fieldSchema()
      // avro stores times as either millis since midnight or micros since midnight
      return when (schema.logicalType) {
         is LogicalTypes.TimeMicros -> LocalTime.ofNanoOfDay(decoder.decodeInt() * 1000L)
         is LogicalTypes.TimeMillis -> LocalTime.ofNanoOfDay(decoder.decodeInt() * 1000000L)
         else -> throw SerializationException("Unsupported logical type for LocalTime [${schema.logicalType}]")
      }
   }
}


@Serializer(forClass = LocalDateTime::class)
class LocalDateTimeSerializer : AvroSerializer<LocalDateTime>() {

   companion object {
      const val name = "java.time.LocalDateTime"
   }

   override val descriptor: SerialDescriptor = StringDescriptor.withName(name)

   override fun toAvroValue(schema: Schema, value: LocalDateTime): Any =
      InstantSerializer().toAvroValue(schema, value.toInstant(ZoneOffset.UTC))

   override fun fromAvroValue(schema: Schema, decoder: Decoder): LocalDateTime =
      LocalDateTime.ofInstant(Instant.ofEpochMilli(decoder.decodeLong()), ZoneOffset.UTC)
}

@Serializer(forClass = Timestamp::class)
class TimestampSerializer : AvroSerializer<Timestamp>() {

   companion object {
      const val name = "java.sql.Timestamp"
   }

   override val descriptor: SerialDescriptor = StringDescriptor.withName(name)

   override fun toAvroValue(schema: Schema, value: Timestamp): Any =
      InstantSerializer().toAvroValue(schema, value.toInstant())

   override fun fromAvroValue(schema: Schema, decoder: Decoder): Timestamp = Timestamp(decoder.decodeLong())
}

@Serializer(forClass = Instant::class)
class InstantSerializer : AvroSerializer<Instant>() {

   companion object {
      const val name = "java.time.Instant"
   }

   override val descriptor: SerialDescriptor = StringDescriptor.withName(name)

   override fun toAvroValue(schema: Schema, value: Instant): Long = value.toEpochMilli()
   override fun fromAvroValue(schema: Schema, decoder: Decoder): Instant = Instant.ofEpochMilli(decoder.decodeLong())
}