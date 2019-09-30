package com.sksamuel.avro4k.serializers

import kotlinx.serialization.Decoder
import kotlinx.serialization.Encoder
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerialDescriptor
import kotlinx.serialization.Serializer
import kotlinx.serialization.internal.StringDescriptor
import kotlinx.serialization.withName
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.util.*

@Serializer(forClass = LocalDate::class)
class LocalDateSerializer : KSerializer<LocalDate> {

   companion object {
      const val name = "java.time.LocalDate"
   }

   override val descriptor: SerialDescriptor = StringDescriptor.withName(name)

   override fun serialize(encoder: Encoder, obj: LocalDate) = TODO()
   override fun deserialize(decoder: Decoder): LocalDate = TODO()
}

@Serializer(forClass = LocalTime::class)
class LocalTimeSerializer : KSerializer<LocalTime> {

   companion object {
      const val name = "java.time.LocalTime"
   }

   override val descriptor: SerialDescriptor = StringDescriptor.withName(name)

   override fun serialize(encoder: Encoder, obj: LocalTime) = TODO()
   override fun deserialize(decoder: Decoder): LocalTime = TODO()
}

@Serializer(forClass = LocalDateTime::class)
class LocalDateTimeSerializer : KSerializer<LocalDateTime> {

   companion object {
      const val name = "java.time.LocalDateTime"
   }

   override val descriptor: SerialDescriptor = StringDescriptor.withName(name)

   override fun serialize(encoder: Encoder, obj: LocalDateTime) = TODO()
   override fun deserialize(decoder: Decoder): LocalDateTime = TODO()
}

@Serializer(forClass = Timestamp::class)
class TimestampSerializer : KSerializer<Timestamp> {

   companion object {
      const val name = "java.sql.Timestamp"
   }

   override val descriptor: SerialDescriptor = StringDescriptor.withName(name)

   override fun serialize(encoder: Encoder, obj: Timestamp) = TODO()
   override fun deserialize(decoder: Decoder): Timestamp = TODO()
}

@Serializer(forClass = Instant::class)
class InstantSerializer : KSerializer<Instant> {

   companion object {
      const val name = "java.time.Instant"
   }

   override val descriptor: SerialDescriptor = StringDescriptor.withName(name)

   override fun serialize(encoder: Encoder, obj: Instant) = TODO()
   override fun deserialize(decoder: Decoder): Instant = TODO()
}

@Serializer(forClass = Date::class)
class DateSerializer : KSerializer<Date> {

   companion object {
      const val name = "java.util.Date"
   }

   override val descriptor: SerialDescriptor = StringDescriptor.withName(name)

   override fun serialize(encoder: Encoder, obj: Date) = TODO()
   override fun deserialize(decoder: Decoder): Date = TODO()
}