package com.github.avrokotlin.avro4k.serializer

import com.github.avrokotlin.avro4k.AnnotatedLocation
import com.github.avrokotlin.avro4k.AvroLogicalType
import com.github.avrokotlin.avro4k.AvroLogicalTypeSupplier
import com.github.avrokotlin.avro4k.decoder.AvroDecoder
import com.github.avrokotlin.avro4k.decoder.decodeResolvingUnion
import com.github.avrokotlin.avro4k.encoder.AvroEncoder
import com.github.avrokotlin.avro4k.encoder.encodeResolvingUnion
import com.github.avrokotlin.avro4k.internal.BadDecodedValueError
import com.github.avrokotlin.avro4k.internal.BadEncodedValueError
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.buildSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import org.apache.avro.LogicalType
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
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
        encoder.encodeResolvingUnion({
            with(encoder) {
                BadEncodedValueError(value, encoder.currentWriterSchema, Schema.Type.INT, Schema.Type.LONG)
            }
        }) { schema ->
            when (schema.type) {
                Schema.Type.INT ->
                    when (schema.logicalType) {
                        is LogicalTypes.Date, null -> {
                            { encoder.encodeInt(value.toEpochDay().toInt()) }
                        }

                        else -> null
                    }

                Schema.Type.LONG ->
                    when (schema.logicalType) {
                        // Date is not compatible with LONG, so we require a null logical type to encode the timestamp
                        null -> {
                            { encoder.encodeLong(value.toEpochDay()) }
                        }

                        else -> null
                    }

                else -> null
            }
        }
    }

    override fun serializeGeneric(
        encoder: Encoder,
        value: LocalDate,
    ) {
        encoder.encodeInt(value.toEpochDay().toInt())
    }

    override fun deserializeAvro(decoder: AvroDecoder): LocalDate {
        return decoder.decodeResolvingUnion({
            with(decoder) {
                BadDecodedValueError(decoder.decodeValue(), decoder.currentWriterSchema, Schema.Type.INT, Schema.Type.LONG)
            }
        }) {
            when (it.type) {
                Schema.Type.INT -> {
                    when (it.logicalType) {
                        is LogicalTypes.Date, null -> {
                            { LocalDate.ofEpochDay(decoder.decodeInt().toLong()) }
                        }

                        else -> null
                    }
                }

                Schema.Type.LONG -> {
                    when (it.logicalType) {
                        null -> {
                            { LocalDate.ofEpochDay(decoder.decodeLong()) }
                        }

                        else -> null
                    }
                }

                else -> null
            }
        }
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
        encoder.encodeResolvingUnion({
            with(encoder) {
                BadEncodedValueError(value, encoder.currentWriterSchema, Schema.Type.INT, Schema.Type.LONG)
            }
        }) { schema ->
            when (schema.type) {
                Schema.Type.INT ->
                    when (schema.logicalType) {
                        is LogicalTypes.TimeMillis, null -> {
                            { encoder.encodeInt(value.toMillisOfDay()) }
                        }

                        else -> null
                    }

                Schema.Type.LONG ->
                    when (schema.logicalType) {
                        // TimeMillis is not compatible with LONG, so we require a null logical type to encode the timestamp
                        null -> {
                            { encoder.encodeLong(value.toMillisOfDay().toLong()) }
                        }

                        is LogicalTypes.TimeMicros -> {
                            { encoder.encodeLong(value.toMicroOfDay()) }
                        }

                        else -> null
                    }

                else -> null
            }
        }
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
        return decoder.decodeResolvingUnion({
            with(decoder) {
                BadDecodedValueError(decoder.decodeValue(), decoder.currentWriterSchema, Schema.Type.INT, Schema.Type.LONG)
            }
        }) {
            when (it.type) {
                Schema.Type.INT -> {
                    when (it.logicalType) {
                        is LogicalTypes.TimeMillis, null -> {
                            { LocalTime.ofNanoOfDay(decoder.decodeInt() * 1_000_000L) }
                        }

                        else -> null
                    }
                }

                Schema.Type.LONG -> {
                    when (it.logicalType) {
                        null -> {
                            { LocalTime.ofNanoOfDay(decoder.decodeLong() * 1_000_000) }
                        }

                        is LogicalTypes.TimeMicros -> {
                            { LocalTime.ofNanoOfDay(decoder.decodeLong() * 1_000) }
                        }

                        else -> null
                    }
                }

                else -> null
            }
        }
    }

    override fun deserializeGeneric(decoder: Decoder): LocalTime {
        return LocalTime.ofNanoOfDay(decoder.decodeInt() * 1_000_000L)
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
        InstantSerializer.serializeAvro(encoder, value.toInstant(ZoneOffset.UTC))
    }

    override fun serializeGeneric(
        encoder: Encoder,
        value: LocalDateTime,
    ) {
        InstantSerializer.serializeGeneric(encoder, value.toInstant(ZoneOffset.UTC))
    }

    override fun deserializeAvro(decoder: AvroDecoder): LocalDateTime {
        return LocalDateTime.ofInstant(InstantSerializer.deserializeAvro(decoder), ZoneOffset.UTC)
    }

    override fun deserializeGeneric(decoder: Decoder): LocalDateTime {
        return LocalDateTime.ofInstant(InstantSerializer.deserializeGeneric(decoder), ZoneOffset.UTC)
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
        encoder.encodeResolvingUnion({
            with(encoder) {
                BadEncodedValueError(value, encoder.currentWriterSchema, Schema.Type.LONG)
            }
        }) {
            when (it.type) {
                Schema.Type.LONG ->
                    when (it.logicalType) {
                        is LogicalTypes.TimestampMillis, null -> {
                            { encoder.encodeLong(value.toEpochMilli()) }
                        }

                        is LogicalTypes.TimestampMicros -> {
                            { encoder.encodeLong(value.toEpochMicros()) }
                        }

                        else -> null
                    }

                else -> null
            }
        }
    }

    override fun serializeGeneric(
        encoder: Encoder,
        value: Instant,
    ) {
        encoder.encodeLong(value.toEpochMilli())
    }

    override fun deserializeAvro(decoder: AvroDecoder): Instant =
        decoder.decodeResolvingUnion({
            with(decoder) {
                BadDecodedValueError(decoder.decodeValue(), decoder.currentWriterSchema, Schema.Type.LONG)
            }
        }) {
            when (it.type) {
                Schema.Type.LONG -> when (it.logicalType) {
                    is LogicalTypes.TimestampMillis, null -> {
                        { Instant.ofEpochMilli(decoder.decodeLong()) }
                    }

                    is LogicalTypes.TimestampMicros -> {
                        { Instant.EPOCH.plus(decoder.decodeLong(), ChronoUnit.MICROS) }
                    }

                    else -> null
                }

                else -> null
            }
        }

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
        encoder.encodeResolvingUnion({
            with(encoder) {
                BadEncodedValueError(value, encoder.currentWriterSchema, Schema.Type.LONG)
            }
        }) { schema ->
            when (schema.type) {
                Schema.Type.LONG ->
                    when (schema.logicalType) {
                        is LogicalTypes.TimestampMicros, null -> {
                            { encoder.encodeLong(value.toEpochMicros()) }
                        }

                        is LogicalTypes.TimestampMillis -> {
                            { encoder.encodeLong(value.toEpochMilli()) }
                        }

                        else -> null
                    }

                else -> null
            }
        }
    }

    override fun serializeGeneric(
        encoder: Encoder,
        value: Instant,
    ) {
        encoder.encodeLong(value.toEpochMicros())
    }

    override fun deserializeAvro(decoder: AvroDecoder): Instant {
        return decoder.decodeResolvingUnion({
            with(decoder) {
                BadDecodedValueError(decoder.decodeValue(), decoder.currentWriterSchema, Schema.Type.LONG)
            }
        }) {
            when (it.type) {
                Schema.Type.LONG -> when (it.logicalType) {
                    is LogicalTypes.TimestampMicros, null -> {
                        { Instant.EPOCH.plus(decoder.decodeLong(), ChronoUnit.MICROS) }
                    }

                    is LogicalTypes.TimestampMillis -> {
                        { Instant.ofEpochMilli(decoder.decodeLong()) }
                    }

                    else -> null
                }

                else -> null
            }
        }
    }

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

private fun Instant.toEpochMicros() = ChronoUnit.MICROS.between(Instant.EPOCH, this)