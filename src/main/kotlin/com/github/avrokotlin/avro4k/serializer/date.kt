package com.github.avrokotlin.avro4k.serializer

import com.github.avrokotlin.avro4k.AnnotatedLocation
import com.github.avrokotlin.avro4k.AvroLogicalType
import com.github.avrokotlin.avro4k.AvroLogicalTypeSupplier
import com.github.avrokotlin.avro4k.decoder.ExtendedDecoder
import com.github.avrokotlin.avro4k.encoder.ExtendedEncoder
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.buildSerialDescriptor
import org.apache.avro.LogicalType
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

object LocalDateSerializer : AvroTimeSerializer<LocalDate>(LocalDate::class, PrimitiveKind.INT) {
    override fun getLogicalType(inlinedStack: List<AnnotatedLocation>): LogicalType {
        return LogicalTypes.date()
    }

    override fun encodeAvroValue(
        schema: Schema,
        encoder: ExtendedEncoder,
        obj: LocalDate,
    ) = encoder.encodeInt(obj.toEpochDay().toInt())

    override fun decodeAvroValue(
        schema: Schema,
        decoder: ExtendedDecoder,
    ): LocalDate = LocalDate.ofEpochDay(decoder.decodeInt().toLong())
}

object LocalTimeSerializer : AvroTimeSerializer<LocalTime>(LocalTime::class, PrimitiveKind.INT) {
    override fun getLogicalType(inlinedStack: List<AnnotatedLocation>): LogicalType {
        return LogicalTypes.timeMillis()
    }

    override fun encodeAvroValue(
        schema: Schema,
        encoder: ExtendedEncoder,
        obj: LocalTime,
    ) = encoder.encodeInt(obj.toSecondOfDay() * 1000 + obj.nano / 1000)

    override fun decodeAvroValue(
        schema: Schema,
        decoder: ExtendedDecoder,
    ): LocalTime {
        // avro stores times as either millis since midnight or micros since midnight
        return when (schema.logicalType) {
            is LogicalTypes.TimeMicros -> LocalTime.ofNanoOfDay(decoder.decodeInt() * 1000L)
            is LogicalTypes.TimeMillis -> LocalTime.ofNanoOfDay(decoder.decodeInt() * 1000000L)
            else -> throw SerializationException("Unsupported logical type for LocalTime [${schema.logicalType}]")
        }
    }
}

object LocalDateTimeSerializer : AvroTimeSerializer<LocalDateTime>(LocalDateTime::class, PrimitiveKind.LONG) {
    override fun getLogicalType(inlinedStack: List<AnnotatedLocation>): LogicalType {
        return LogicalTypes.timestampMillis()
    }

    override fun encodeAvroValue(
        schema: Schema,
        encoder: ExtendedEncoder,
        obj: LocalDateTime,
    ) = InstantSerializer.encodeAvroValue(schema, encoder, obj.toInstant(ZoneOffset.UTC))

    override fun decodeAvroValue(
        schema: Schema,
        decoder: ExtendedDecoder,
    ): LocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(decoder.decodeLong()), ZoneOffset.UTC)
}

object TimestampSerializer : AvroTimeSerializer<Timestamp>(Timestamp::class, PrimitiveKind.LONG) {
    override fun getLogicalType(inlinedStack: List<AnnotatedLocation>): LogicalType {
        return LogicalTypes.timestampMillis()
    }

    override fun encodeAvroValue(
        schema: Schema,
        encoder: ExtendedEncoder,
        obj: Timestamp,
    ) = InstantSerializer.encodeAvroValue(schema, encoder, obj.toInstant())

    override fun decodeAvroValue(
        schema: Schema,
        decoder: ExtendedDecoder,
    ): Timestamp = Timestamp(decoder.decodeLong())
}

object InstantSerializer : AvroTimeSerializer<Instant>(Instant::class, PrimitiveKind.LONG) {
    override fun getLogicalType(inlinedStack: List<AnnotatedLocation>): LogicalType {
        return LogicalTypes.timestampMillis()
    }

    override fun encodeAvroValue(
        schema: Schema,
        encoder: ExtendedEncoder,
        obj: Instant,
    ) = encoder.encodeLong(obj.toEpochMilli())

    override fun decodeAvroValue(
        schema: Schema,
        decoder: ExtendedDecoder,
    ): Instant = Instant.ofEpochMilli(decoder.decodeLong())
}

object InstantToMicroSerializer : AvroTimeSerializer<Instant>(Instant::class, PrimitiveKind.LONG) {
    override fun getLogicalType(inlinedStack: List<AnnotatedLocation>): LogicalType {
        return LogicalTypes.timestampMicros()
    }

    override fun encodeAvroValue(
        schema: Schema,
        encoder: ExtendedEncoder,
        obj: Instant,
    ) = encoder.encodeLong(ChronoUnit.MICROS.between(Instant.EPOCH, obj))

    override fun decodeAvroValue(
        schema: Schema,
        decoder: ExtendedDecoder,
    ): Instant = Instant.EPOCH.plus(decoder.decodeLong(), ChronoUnit.MICROS)
}

@OptIn(InternalSerializationApi::class, ExperimentalSerializationApi::class)
abstract class AvroTimeSerializer<T : Any>(
    klass: KClass<T>,
    kind: PrimitiveKind,
) : AvroSerializer<T>(), AvroLogicalTypeSupplier {
    override val descriptor =
        buildSerialDescriptor(klass.qualifiedName!!, kind) {
            annotations = listOf(AvroLogicalType(this@AvroTimeSerializer::class))
        }
}