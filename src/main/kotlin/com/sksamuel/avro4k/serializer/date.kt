package com.sksamuel.avro4k.serializer

import com.sksamuel.avro4k.schema.AvroDescriptor
import kotlinx.serialization.Decoder
import kotlinx.serialization.PrimitiveKind
import kotlinx.serialization.SerialDescriptor
import kotlinx.serialization.SerializationException
import kotlinx.serialization.Serializer
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneOffset
import kotlin.reflect.jvm.jvmName

@Serializer(forClass = LocalDate::class)
class LocalDateSerializer : AvroSerializer<LocalDate>() {

   override val descriptor: SerialDescriptor = object : AvroDescriptor(LocalDate::class.jvmName, PrimitiveKind.INT) {
      override fun schema(annos: List<Annotation>): Schema {
         val schema = SchemaBuilder.builder().intType()
         return LogicalTypes.date().addToSchema(schema)
      }
   }

   override fun toAvroValue(schema: Schema, value: LocalDate): Int = value.toEpochDay().toInt()
   override fun fromAvroValue(schema: Schema, decoder: Decoder): LocalDate = LocalDate.ofEpochDay(decoder.decodeLong())
}

@Serializer(forClass = LocalTime::class)
class LocalTimeSerializer : AvroSerializer<LocalTime>() {

   override val descriptor: SerialDescriptor = object : AvroDescriptor(LocalTime::class.jvmName, PrimitiveKind.INT) {
      override fun schema(annos: List<Annotation>): Schema {
         val schema = SchemaBuilder.builder().intType()
         return LogicalTypes.timeMillis().addToSchema(schema)
      }
   }

   override fun toAvroValue(schema: Schema, value: LocalTime): Int {
      return value.toSecondOfDay() * 1000 + value.nano / 1000
   }

   override fun fromAvroValue(schema: Schema, decoder: Decoder): LocalTime {
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

   override val descriptor: SerialDescriptor =
      object : AvroDescriptor(LocalDateTime::class.jvmName, PrimitiveKind.LONG) {
         override fun schema(annos: List<Annotation>): Schema {
            val schema = SchemaBuilder.builder().longType()
            return LogicalTypes.timestampMillis().addToSchema(schema)
         }
   }

   override fun toAvroValue(schema: Schema, value: LocalDateTime): Any =
      InstantSerializer().toAvroValue(schema, value.toInstant(ZoneOffset.UTC))

   override fun fromAvroValue(schema: Schema, decoder: Decoder): LocalDateTime =
      LocalDateTime.ofInstant(Instant.ofEpochMilli(decoder.decodeLong()), ZoneOffset.UTC)
}

@Serializer(forClass = Timestamp::class)
class TimestampSerializer : AvroSerializer<Timestamp>() {

   override val descriptor: SerialDescriptor = object : AvroDescriptor(Timestamp::class.jvmName, PrimitiveKind.LONG) {
      override fun schema(annos: List<Annotation>): Schema {
         val schema = SchemaBuilder.builder().longType()
         return LogicalTypes.timestampMillis().addToSchema(schema)
      }
   }

   override fun toAvroValue(schema: Schema, value: Timestamp): Any =
      InstantSerializer().toAvroValue(schema, value.toInstant())

   override fun fromAvroValue(schema: Schema, decoder: Decoder): Timestamp = Timestamp(decoder.decodeLong())
}

@Serializer(forClass = Instant::class)
class InstantSerializer : AvroSerializer<Instant>() {

   override val descriptor: SerialDescriptor = object : AvroDescriptor(Instant::class.jvmName, PrimitiveKind.LONG) {
      override fun schema(annos: List<Annotation>): Schema {
         val schema = SchemaBuilder.builder().longType()
         return LogicalTypes.timestampMillis().addToSchema(schema)
      }
   }

   override fun toAvroValue(schema: Schema, value: Instant): Long = value.toEpochMilli()
   override fun fromAvroValue(schema: Schema, decoder: Decoder): Instant = Instant.ofEpochMilli(decoder.decodeLong())
}