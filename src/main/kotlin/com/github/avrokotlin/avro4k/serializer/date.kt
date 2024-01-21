@file:OptIn(ExperimentalSerializationApi::class)
package com.github.avrokotlin.avro4k.serializer

import com.github.avrokotlin.avro4k.AvroTimeLogicalType
import com.github.avrokotlin.avro4k.LogicalTimeTypeEnum
import com.github.avrokotlin.avro4k.decoder.ExtendedDecoder
import com.github.avrokotlin.avro4k.encoder.ExtendedEncoder
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.Serializer
import kotlinx.serialization.descriptors.buildSerialDescriptor
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit
import kotlin.reflect.KClass

@OptIn(InternalSerializationApi::class)
private fun buildTimeSerialDescriptor(clazz: KClass<*>, type: LogicalTimeTypeEnum) = buildSerialDescriptor(clazz.qualifiedName!!, type.kind) {
   annotations = listOf(AvroTimeLogicalType(type))
}

@Serializer(forClass = LocalDate::class)
class LocalDateSerializer : AvroSerializer<LocalDate>() {

   override val descriptor = buildTimeSerialDescriptor(LocalDate::class, LogicalTimeTypeEnum.DATE)

   override fun encodeAvroValue(schema: Schema, encoder: ExtendedEncoder, obj: LocalDate) =
      encoder.encodeInt(obj.toEpochDay().toInt())

   override fun decodeAvroValue(schema: Schema, decoder: ExtendedDecoder): LocalDate = LocalDate.ofEpochDay(decoder.decodeLong())
}

@Serializer(forClass = LocalTime::class)
class LocalTimeSerializer : AvroSerializer<LocalTime>() {

   override val descriptor = buildTimeSerialDescriptor(LocalTime::class, LogicalTimeTypeEnum.TIME_MILLIS)

   override fun encodeAvroValue(schema: Schema, encoder: ExtendedEncoder, obj: LocalTime) =
      encoder.encodeInt(obj.toSecondOfDay() * 1000 + obj.nano / 1000)

   override fun decodeAvroValue(schema: Schema, decoder: ExtendedDecoder): LocalTime {
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

   override val descriptor = buildTimeSerialDescriptor(LocalDateTime::class, LogicalTimeTypeEnum.TIMESTAMP_MILLIS)

   override fun encodeAvroValue(schema: Schema, encoder: ExtendedEncoder, obj: LocalDateTime) =
      InstantSerializer().encodeAvroValue(schema, encoder, obj.toInstant(ZoneOffset.UTC))

   override fun decodeAvroValue(schema: Schema, decoder: ExtendedDecoder): LocalDateTime =
      LocalDateTime.ofInstant(Instant.ofEpochMilli(decoder.decodeLong()), ZoneOffset.UTC)
}
@Serializer(forClass = Timestamp::class)
class TimestampSerializer : AvroSerializer<Timestamp>() {

   override val descriptor = buildTimeSerialDescriptor(Timestamp::class, LogicalTimeTypeEnum.TIMESTAMP_MILLIS)

   override fun encodeAvroValue(schema: Schema, encoder: ExtendedEncoder, obj: Timestamp) =
      InstantSerializer().encodeAvroValue(schema, encoder, obj.toInstant())

   override fun decodeAvroValue(schema: Schema, decoder: ExtendedDecoder): Timestamp = Timestamp(decoder.decodeLong())
}
@Serializer(forClass = Instant::class)
class InstantSerializer : AvroSerializer<Instant>() {

   override val descriptor = buildTimeSerialDescriptor(Instant::class, LogicalTimeTypeEnum.TIMESTAMP_MILLIS)

   override fun encodeAvroValue(schema: Schema, encoder: ExtendedEncoder, obj: Instant) =
      encoder.encodeLong(obj.toEpochMilli())

   override fun decodeAvroValue(schema: Schema, decoder: ExtendedDecoder): Instant = Instant.ofEpochMilli(decoder.decodeLong())
}

@Serializer(forClass = Instant::class)
class InstantToMicroSerializer : AvroSerializer<Instant>() {

   override val descriptor = buildTimeSerialDescriptor(Instant::class, LogicalTimeTypeEnum.TIMESTAMP_MICROS)

   override fun encodeAvroValue(schema: Schema, encoder: ExtendedEncoder, obj: Instant) =
      encoder.encodeLong(ChronoUnit.MICROS.between(Instant.EPOCH, obj))

   override fun decodeAvroValue(schema: Schema, decoder: ExtendedDecoder): Instant =
      Instant.EPOCH.plus(decoder.decodeLong(), ChronoUnit.MICROS)
}
