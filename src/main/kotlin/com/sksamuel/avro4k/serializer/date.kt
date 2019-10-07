package com.sksamuel.avro4k.serializer

import kotlinx.serialization.Decoder
import kotlinx.serialization.Encoder
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerialDescriptor
import kotlinx.serialization.Serializer
import kotlinx.serialization.internal.IntDescriptor
import kotlinx.serialization.internal.StringDescriptor
import kotlinx.serialization.withName
import org.apache.avro.Schema
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneOffset
import java.util.*

@Serializer(forClass = LocalDate::class)
class LocalDateSerializer : AvroSerializer<LocalDate>() {

   companion object {
      const val name = "java.time.LocalDate"
   }

   override val descriptor: SerialDescriptor = StringDescriptor.withName(name)

   override fun toAvroValue(schema: Schema, value: LocalDate): Any = value.toEpochDay().toInt()
   override fun deserialize(decoder: Decoder): LocalDate = TODO()
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

   //    val schema = (encoder as FieldEncoder).fieldSchema()
   //  // avro stores times as either millis since midnight or micros since midnight
//      when (schema.logicalType) {
//         is LogicalTypes.TimeMicros -> when (value) {
//            is Int -> LocalTime.ofNanoOfDay(i.toLong * 1000L)
//            is Long -> LocalTime.ofNanoOfDay(l * 1000L)
//         }
//         is LogicalTypes.TimeMillis ->
//            value match {
//               case i : Int => LocalTime . ofNanoOfDay (i.toLong * 1000000L)
//               case l : Long => LocalTime . ofNanoOfDay (l * 1000000L)
//            }
//      }

   override fun deserialize(decoder: Decoder): LocalTime = TODO()
}


@Serializer(forClass = LocalDateTime::class)
class LocalDateTimeSerializer : AvroSerializer<LocalDateTime>() {

   companion object {
      const val name = "java.time.LocalDateTime"
   }

   override val descriptor: SerialDescriptor = StringDescriptor.withName(name)

   override fun toAvroValue(schema: Schema, value: LocalDateTime): Any =
      InstantSerializer().toAvroValue(schema, value.toInstant(ZoneOffset.UTC))

   override fun deserialize(decoder: Decoder): LocalDateTime = TODO()
}

@Serializer(forClass = Timestamp::class)
class TimestampSerializer : KSerializer<Timestamp> {

   companion object {
      const val name = "java.sql.Timestamp"
   }

   override val descriptor: SerialDescriptor = StringDescriptor.withName(name)

   override fun serialize(encoder: Encoder, obj: Timestamp) =
      InstantSerializer().serialize(encoder, obj.toInstant())

   override fun deserialize(decoder: Decoder): Timestamp = TODO()
}

@Serializer(forClass = Instant::class)
class InstantSerializer : AvroSerializer<Instant>() {

   companion object {
      const val name = "java.time.Instant"
   }

   override val descriptor: SerialDescriptor = StringDescriptor.withName(name)

   override fun toAvroValue(schema: Schema, value: Instant): Any = value.toEpochMilli()
   override fun deserialize(decoder: Decoder): Instant = TODO()
}

@Serializer(forClass = java.sql.Date::class)
class SqlDateSerializer : KSerializer<java.sql.Date> {

   companion object {
      const val name = "java.sql.Date"
   }

   override val descriptor: SerialDescriptor = StringDescriptor.withName(name)

   override fun serialize(encoder: Encoder, obj: java.sql.Date) =
      LocalDateSerializer().serialize(encoder, obj.toLocalDate())

   override fun deserialize(decoder: Decoder): java.sql.Date = TODO()
}

@Serializer(forClass = Date::class)
class DateSerializer : KSerializer<Date> {

   companion object {
      const val name = "java.util.Date"
   }

   override val descriptor: SerialDescriptor = StringDescriptor.withName(name)

   override fun serialize(encoder: Encoder, obj: Date) =
      InstantSerializer().serialize(encoder, Instant.ofEpochMilli(obj.time))

   override fun deserialize(decoder: Decoder): Date = TODO()
}