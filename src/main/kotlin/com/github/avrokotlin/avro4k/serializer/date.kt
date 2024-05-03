package com.github.avrokotlin.avro4k.serializer

import com.github.avrokotlin.avro4k.AnnotatedLocation
import com.github.avrokotlin.avro4k.AvroLogicalType
import com.github.avrokotlin.avro4k.AvroLogicalTypeSupplier
import com.github.avrokotlin.avro4k.decoder.AvroDecoder
import com.github.avrokotlin.avro4k.encoder.AvroEncoder
import com.github.avrokotlin.avro4k.encoder.SchemaTypeMatcher
import com.github.avrokotlin.avro4k.encoder.encodeValueResolved
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.buildSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import org.apache.avro.LogicalType
import org.apache.avro.LogicalTypes
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit
import kotlin.reflect.KClass

public object LocalDateSerializer : AvroTimeSerializer<LocalDate>(LocalDate::class, PrimitiveKind.INT) {
    override fun getLogicalType(inlinedStack: List<AnnotatedLocation>): LogicalType {
        return LogicalTypes.date()
    }

    override fun serializeAvro(
        encoder: AvroEncoder,
        value: LocalDate,
    ) {
        encoder.encodeValueResolved<LocalTime>(
            SchemaTypeMatcher.Scalar.INT to {
                when (it.logicalType) {
                    is LogicalTypes.Date, null -> value.toEpochDay().toInt()
                    else -> it.logicalType.throwUnsupportedWith<LocalDate>()
                }
            },
            SchemaTypeMatcher.Scalar.LONG to {
                when (it.logicalType) {
                    // Date is not compatible with LONG, so we require a null logical type to encode the timestamp
                    null -> value.toEpochDay()
                    else -> it.logicalType.throwUnsupportedWith<LocalDate>()
                }
            },
            SchemaTypeMatcher.Scalar.STRING to { value.toString() }
        )
    }

    override fun serializeGeneric(
        encoder: Encoder,
        value: LocalDate,
    ) {
        encoder.encodeInt(value.toEpochDay().toInt())
    }

    override fun deserializeAvro(decoder: AvroDecoder): LocalDate {
        return deserializeGeneric(decoder)
    }

    override fun deserializeGeneric(decoder: Decoder): LocalDate {
        return LocalDate.ofEpochDay(decoder.decodeInt().toLong())
    }
}

public object LocalTimeSerializer : AvroTimeSerializer<LocalTime>(LocalTime::class, PrimitiveKind.INT) {
    override fun getLogicalType(inlinedStack: List<AnnotatedLocation>): LogicalType {
        return LogicalTypes.timeMillis()
    }

    override fun serializeAvro(
        encoder: AvroEncoder,
        value: LocalTime,
    ) {
        encoder.encodeValueResolved<LocalTime>(
            SchemaTypeMatcher.Scalar.INT to {
                when (it.logicalType) {
                    is LogicalTypes.TimeMillis, null -> value.toMillisOfDay()
                    else -> it.logicalType.throwUnsupportedWith<LocalTime>()
                }
            },
            SchemaTypeMatcher.Scalar.LONG to {
                when (it.logicalType) {
                    // TimeMillis is not compatible with LONG, so we require a null logical type to encode the timestamp
                    null -> value.toMillisOfDay().toLong()
                    is LogicalTypes.TimeMicros -> value.toMicroOfDay()
                    else -> it.logicalType.throwUnsupportedWith<LocalTime>()
                }
            },
            SchemaTypeMatcher.Scalar.STRING to { value.truncatedTo(ChronoUnit.MILLIS).toString() }
        )
    }

    override fun serializeGeneric(
        encoder: Encoder,
        value: LocalTime,
    ) {
        encoder.encodeInt(value.toMillisOfDay())
    }

    private fun LocalTime.toMillisOfDay() = (toNanoOfDay() / 1000000).toInt()

    private fun LocalTime.toMicroOfDay() = toNanoOfDay() / 1000

    override fun deserializeAvro(decoder: AvroDecoder): LocalTime {
        // avro stores times as either millis since midnight or micros since midnight
        return when (decoder.currentWriterSchema.logicalType) {
            is LogicalTypes.TimeMicros -> LocalTime.ofNanoOfDay(decoder.decodeInt() * 1000L)
            is LogicalTypes.TimeMillis -> LocalTime.ofNanoOfDay(decoder.decodeInt() * 1000000L)
            else -> throw SerializationException("Unsupported logical type for LocalTime [${decoder.currentWriterSchema.logicalType}]")
        }
    }

    override fun deserializeGeneric(decoder: Decoder): LocalTime {
        return LocalTime.ofNanoOfDay(decoder.decodeInt() * 1000000L)
    }
}

public object LocalDateTimeSerializer : AvroTimeSerializer<LocalDateTime>(LocalDateTime::class, PrimitiveKind.LONG) {
    override fun getLogicalType(inlinedStack: List<AnnotatedLocation>): LogicalType {
        return LogicalTypes.timestampMillis()
    }

    override fun serializeAvro(
        encoder: AvroEncoder,
        value: LocalDateTime,
    ) {
        encoder.encodeValueResolved<LocalDateTime>(
            SchemaTypeMatcher.Scalar.LONG to {
                when (it.logicalType) {
                    is LogicalTypes.TimestampMillis, null -> value.toInstant(ZoneOffset.UTC).toEpochMilli()
                    is LogicalTypes.TimestampMicros -> value.toInstant(ZoneOffset.UTC).toEpochMicros()
                    else -> it.logicalType.throwUnsupportedWith<LocalDateTime>()
                }
            },
            SchemaTypeMatcher.Scalar.STRING to { value.truncatedTo(ChronoUnit.MILLIS).toString() }
        )
    }

    override fun serializeGeneric(
        encoder: Encoder,
        value: LocalDateTime,
    ) {
        encoder.encodeLong(value.toInstant(ZoneOffset.UTC).toEpochMilli())
    }

    override fun deserializeAvro(decoder: AvroDecoder): LocalDateTime = deserializeGeneric(decoder)

    override fun deserializeGeneric(decoder: Decoder): LocalDateTime {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(decoder.decodeLong()), ZoneOffset.UTC)
    }
}

public object TimestampSerializer : AvroTimeSerializer<Timestamp>(Timestamp::class, PrimitiveKind.LONG) {
    override fun getLogicalType(inlinedStack: List<AnnotatedLocation>): LogicalType {
        return LogicalTypes.timestampMillis()
    }

    override fun serializeAvro(
        encoder: AvroEncoder,
        value: Timestamp,
    ) {
        encoder.encodeValueResolved<Timestamp>(
            SchemaTypeMatcher.Scalar.LONG to {
                when (it.logicalType) {
                    is LogicalTypes.TimestampMillis, null -> value.toInstant().toEpochMilli()
                    is LogicalTypes.TimestampMicros -> value.toInstant().toEpochMicros()
                    else -> it.logicalType.throwUnsupportedWith<Timestamp>()
                }
            },
            SchemaTypeMatcher.Scalar.STRING to { value.toInstant().toString() }
        )
    }

    override fun serializeGeneric(
        encoder: Encoder,
        value: Timestamp,
    ) {
        encoder.encodeLong(value.toInstant().toEpochMilli())
    }

    override fun deserializeAvro(decoder: AvroDecoder): Timestamp = deserializeGeneric(decoder)

    override fun deserializeGeneric(decoder: Decoder): Timestamp {
        return Timestamp(decoder.decodeLong())
    }
}

public object InstantSerializer : AvroTimeSerializer<Instant>(Instant::class, PrimitiveKind.LONG) {
    override fun getLogicalType(inlinedStack: List<AnnotatedLocation>): LogicalType {
        return LogicalTypes.timestampMillis()
    }

    override fun serializeAvro(
        encoder: AvroEncoder,
        value: Instant,
    ) {
        encoder.encodeValueResolved<Instant>(
            SchemaTypeMatcher.Scalar.LONG to {
                when (it.logicalType) {
                    is LogicalTypes.TimestampMillis, null -> value.toEpochMilli()
                    is LogicalTypes.TimestampMicros -> value.toEpochMicros()
                    else -> it.logicalType.throwUnsupportedWith<Instant>()
                }
            },
            SchemaTypeMatcher.Scalar.STRING to { value.toString() }
        )
    }

    override fun serializeGeneric(
        encoder: Encoder,
        value: Instant,
    ) {
        encoder.encodeLong(value.toEpochMilli())
    }

    override fun deserializeAvro(decoder: AvroDecoder): Instant = deserializeGeneric(decoder)

    override fun deserializeGeneric(decoder: Decoder): Instant {
        return Instant.ofEpochMilli(decoder.decodeLong())
    }
}

public object InstantToMicroSerializer : AvroTimeSerializer<Instant>(Instant::class, PrimitiveKind.LONG) {
    override fun getLogicalType(inlinedStack: List<AnnotatedLocation>): LogicalType {
        return LogicalTypes.timestampMicros()
    }

    override fun serializeAvro(
        encoder: AvroEncoder,
        value: Instant,
    ) {
        encoder.encodeValueResolved<Instant>(
            SchemaTypeMatcher.Scalar.LONG to {
                when (it.logicalType) {
                    is LogicalTypes.TimestampMicros, null -> value.toEpochMicros()
                    is LogicalTypes.TimestampMillis -> value.toEpochMilli()
                    else -> it.logicalType.throwUnsupportedWith<Instant>()
                }
            },
            SchemaTypeMatcher.Scalar.STRING to { value.toString() }
        )
    }

    override fun serializeGeneric(
        encoder: Encoder,
        value: Instant,
    ) {
        encoder.encodeLong(value.toEpochMicros())
    }

    override fun deserializeAvro(decoder: AvroDecoder): Instant = deserializeGeneric(decoder)

    override fun deserializeGeneric(decoder: Decoder): Instant {
        return Instant.EPOCH.plus(decoder.decodeLong(), ChronoUnit.MICROS)
    }
}

@OptIn(InternalSerializationApi::class, ExperimentalSerializationApi::class)
public abstract class AvroTimeSerializer<T : Any>(
    klass: KClass<T>,
    kind: PrimitiveKind,
) : AvroSerializer<T>(), AvroLogicalTypeSupplier {
    override val descriptor: SerialDescriptor =
        buildSerialDescriptor(klass.qualifiedName!!, kind) {
            annotations = listOf(AvroLogicalType(this@AvroTimeSerializer::class))
        }
}

private inline fun <reified T> LogicalType?.throwUnsupportedWith(): Nothing {
    throw SerializationException("Unsupported logical type $this for kotlin type ${T::class.qualifiedName}")
}

private fun Instant.toEpochMicros() = ChronoUnit.MICROS.between(Instant.EPOCH, this)