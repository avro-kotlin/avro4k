package com.sksamuel.avro4k.serializer

import com.sksamuel.avro4k.decoder.ExtendedDecoder
import com.sksamuel.avro4k.encoder.ExtendedEncoder
import com.sksamuel.avro4k.schema.AvroDescriptor
import com.sksamuel.avro4k.schema.NamingStrategy
import kotlinx.serialization.PrimitiveKind
import kotlinx.serialization.SerialDescriptor
import kotlinx.serialization.SerializationException
import kotlinx.serialization.Serializer
import kotlinx.serialization.modules.SerialModule
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import java.sql.Timestamp
import java.time.*
import kotlin.reflect.jvm.jvmName

@Serializer(forClass = LocalDate::class)
class LocalDateSerializer : AvroSerializer<LocalDate>() {

   override val descriptor: SerialDescriptor = object : AvroDescriptor(LocalDate::class.jvmName, PrimitiveKind.INT) {
      override fun schema(annos: List<Annotation>, context: SerialModule, namingStrategy: NamingStrategy): Schema {
         val schema = SchemaBuilder.builder().intType()
         return LogicalTypes.date().addToSchema(schema)
      }
   }

   override fun encodeAvroValue(schema: Schema, encoder: ExtendedEncoder, obj: LocalDate) =
      encoder.encodeInt(obj.toEpochDay().toInt())

   override fun decodeAvroValue(schema: Schema, decoder: ExtendedDecoder): LocalDate = LocalDate.ofEpochDay(decoder.decodeLong())
}

@Serializer(forClass = LocalTime::class)
class LocalTimeSerializer : AvroSerializer<LocalTime>() {

   override val descriptor: SerialDescriptor = object : AvroDescriptor(LocalTime::class.jvmName, PrimitiveKind.INT) {
      override fun schema(annos: List<Annotation>, context: SerialModule, namingStrategy: NamingStrategy): Schema {
         val schema = SchemaBuilder.builder().intType()
         return LogicalTypes.timeMillis().addToSchema(schema)
      }
   }

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

   override val descriptor: SerialDescriptor =
      object : AvroDescriptor(LocalDateTime::class.jvmName, PrimitiveKind.LONG) {
         override fun schema(annos: List<Annotation>, context: SerialModule, namingStrategy: NamingStrategy): Schema {
            val schema = SchemaBuilder.builder().longType()
            return LogicalTypes.timestampMillis().addToSchema(schema)
         }
   }

   override fun encodeAvroValue(schema: Schema, encoder: ExtendedEncoder, obj: LocalDateTime) =
      InstantSerializer().encodeAvroValue(schema, encoder, obj.toInstant(ZoneOffset.UTC))

   override fun decodeAvroValue(schema: Schema, decoder: ExtendedDecoder): LocalDateTime =
      LocalDateTime.ofInstant(Instant.ofEpochMilli(decoder.decodeLong()), ZoneOffset.UTC)
}

@Serializer(forClass = Timestamp::class)
class TimestampSerializer : AvroSerializer<Timestamp>() {

   override val descriptor: SerialDescriptor = object : AvroDescriptor(Timestamp::class.jvmName, PrimitiveKind.LONG) {
      override fun schema(annos: List<Annotation>, context: SerialModule, namingStrategy: NamingStrategy): Schema {
         val schema = SchemaBuilder.builder().longType()
         return LogicalTypes.timestampMillis().addToSchema(schema)
      }
   }

   override fun encodeAvroValue(schema: Schema, encoder: ExtendedEncoder, obj: Timestamp) =
      InstantSerializer().encodeAvroValue(schema, encoder, obj.toInstant())

   override fun decodeAvroValue(schema: Schema, decoder: ExtendedDecoder): Timestamp = Timestamp(decoder.decodeLong())
}

@Serializer(forClass = Instant::class)
class InstantSerializer : AvroSerializer<Instant>() {

   override val descriptor: SerialDescriptor = object : AvroDescriptor(Instant::class.jvmName, PrimitiveKind.LONG) {
      override fun schema(annos: List<Annotation>, context: SerialModule, namingStrategy: NamingStrategy): Schema {
         val schema = SchemaBuilder.builder().longType()
         return LogicalTypes.timestampMillis().addToSchema(schema)
      }
   }

   override fun encodeAvroValue(schema: Schema, encoder: ExtendedEncoder, obj: Instant) =
      encoder.encodeLong(obj.toEpochMilli())

   override fun decodeAvroValue(schema: Schema, decoder: ExtendedDecoder): Instant = Instant.ofEpochMilli(decoder.decodeLong())
}