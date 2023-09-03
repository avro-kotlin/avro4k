@file:OptIn(ExperimentalSerializationApi::class)

package com.github.avrokotlin.avro4k.serializer

import com.github.avrokotlin.avro4k.decoder.ExtendedDecoder
import com.github.avrokotlin.avro4k.encoder.NativeAvroEncoder
import com.github.avrokotlin.avro4k.schema.AvroDescriptor
import com.github.avrokotlin.avro4k.schema.NamingStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit
import kotlin.reflect.jvm.jvmName

object LocalDateSerializer : AvroSerializer<LocalDate>() {
    override val descriptor: SerialDescriptor = object : AvroDescriptor(LocalDate::class.jvmName, PrimitiveKind.INT) {
        override fun schema(annos: List<Annotation>, serializersModule: SerializersModule, namingStrategy: NamingStrategy): Schema {
            val schema = SchemaBuilder.builder().intType()
            return LogicalTypes.date().addToSchema(schema)
        }
    }

    override fun encodeAvroValue(schema: Schema, encoder: NativeAvroEncoder, obj: LocalDate) =
            encoder.encodeInt(obj.toEpochDay().toInt())

    override fun decodeAvroValue(schema: Schema, decoder: ExtendedDecoder): LocalDate =
            LocalDate.ofEpochDay(decoder.decodeLong())
}

object LocalTimeSerializer : AvroSerializer<LocalTime>() {
    override val descriptor: SerialDescriptor = object : AvroDescriptor(LocalTime::class.jvmName, PrimitiveKind.INT) {
        override fun schema(annos: List<Annotation>, serializersModule: SerializersModule, namingStrategy: NamingStrategy): Schema {
            val schema = SchemaBuilder.builder().intType()
            return LogicalTypes.timeMillis().addToSchema(schema)
        }
    }

    override fun encodeAvroValue(schema: Schema, encoder: NativeAvroEncoder, obj: LocalTime) =
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

object LocalDateTimeSerializer : AvroSerializer<LocalDateTime>() {
    override val descriptor: SerialDescriptor =
            object : AvroDescriptor(LocalDateTime::class.jvmName, PrimitiveKind.LONG) {
                override fun schema(annos: List<Annotation>, serializersModule: SerializersModule, namingStrategy: NamingStrategy): Schema {
                    val schema = SchemaBuilder.builder().longType()
                    return LogicalTypes.timestampMillis().addToSchema(schema)
                }
            }

    override fun encodeAvroValue(schema: Schema, encoder: NativeAvroEncoder, obj: LocalDateTime) =
            InstantSerializer.encodeAvroValue(schema, encoder, obj.toInstant(ZoneOffset.UTC))

    override fun decodeAvroValue(schema: Schema, decoder: ExtendedDecoder): LocalDateTime =
            LocalDateTime.ofInstant(InstantSerializer.decodeAvroValue(schema, decoder), ZoneOffset.UTC)
}

object TimestampSerializer : AvroSerializer<Timestamp>() {
    override val descriptor: SerialDescriptor = object : AvroDescriptor(Timestamp::class.jvmName, PrimitiveKind.LONG) {
        override fun schema(annos: List<Annotation>, serializersModule: SerializersModule, namingStrategy: NamingStrategy): Schema {
            val schema = SchemaBuilder.builder().longType()
            return LogicalTypes.timestampMillis().addToSchema(schema)
        }
    }

    override fun encodeAvroValue(schema: Schema, encoder: NativeAvroEncoder, obj: Timestamp) =
            encoder.encodeLong(obj.time)

    override fun decodeAvroValue(schema: Schema, decoder: ExtendedDecoder): Timestamp =
            Timestamp(decoder.decodeLong())
}

object InstantSerializer : AvroSerializer<Instant>() {
    override val descriptor: SerialDescriptor = object : AvroDescriptor(Instant::class.jvmName, PrimitiveKind.LONG) {
        override fun schema(annos: List<Annotation>, serializersModule: SerializersModule, namingStrategy: NamingStrategy): Schema {
            val schema = SchemaBuilder.builder().longType()
            return LogicalTypes.timestampMillis().addToSchema(schema)
        }
    }

    override fun encodeAvroValue(schema: Schema, encoder: NativeAvroEncoder, obj: Instant) =
            encoder.encodeLong(obj.toEpochMilli())

    override fun decodeAvroValue(schema: Schema, decoder: ExtendedDecoder): Instant =
            Instant.ofEpochMilli(decoder.decodeLong())
}

object InstantToMicroSerializer : AvroSerializer<Instant>() {
    override val descriptor: SerialDescriptor = object : AvroDescriptor(Instant::class.jvmName, PrimitiveKind.LONG) {
        override fun schema(
                annos: List<Annotation>,
                serializersModule: SerializersModule,
                namingStrategy: NamingStrategy
        ): Schema {
            val schema = SchemaBuilder.builder().longType()
            return LogicalTypes.timestampMicros().addToSchema(schema)
        }
    }

    override fun encodeAvroValue(schema: Schema, encoder: NativeAvroEncoder, obj: Instant) =
            encoder.encodeLong(ChronoUnit.MICROS.between(Instant.EPOCH, obj))

    override fun decodeAvroValue(schema: Schema, decoder: ExtendedDecoder): Instant =
            Instant.EPOCH.plus(decoder.decodeLong(), ChronoUnit.MICROS)
}
