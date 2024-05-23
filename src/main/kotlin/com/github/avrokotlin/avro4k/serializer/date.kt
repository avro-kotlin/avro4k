package com.github.avrokotlin.avro4k.serializer

import com.github.avrokotlin.avro4k.asAvroLogicalType
import com.github.avrokotlin.avro4k.decoder.AvroDecoder
import com.github.avrokotlin.avro4k.decoder.decodeResolvingUnion
import com.github.avrokotlin.avro4k.encoder.AvroEncoder
import com.github.avrokotlin.avro4k.encoder.encodeResolvingUnion
import com.github.avrokotlin.avro4k.internal.BadDecodedValueError
import com.github.avrokotlin.avro4k.internal.BadEncodedValueError
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import org.apache.avro.Schema
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit

public object LocalDateSerializer : AvroSerializer<LocalDate>() {
    override val descriptor: SerialDescriptor =
        PrimitiveSerialDescriptor("date", PrimitiveKind.INT).asAvroLogicalType()

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
                    when (schema.logicalType?.name) {
                        "date", null -> {
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
                    when (it.logicalType?.name) {
                        "date", null -> {
                            { LocalDate.ofEpochDay(decoder.decodeInt().toLong()) }
                        }

                        else -> null
                    }
                }

                Schema.Type.LONG -> {
                    when (it.logicalType?.name) {
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

public object LocalTimeSerializer : AvroSerializer<LocalTime>() {
    override val descriptor: SerialDescriptor =
        PrimitiveSerialDescriptor("time-millis", PrimitiveKind.INT).asAvroLogicalType()

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
                    when (schema.logicalType?.name) {
                        "time-millis", null -> {
                            { encoder.encodeInt(value.toMillisOfDay()) }
                        }

                        else -> null
                    }

                Schema.Type.LONG ->
                    when (schema.logicalType?.name) {
                        // TimeMillis is not compatible with LONG, so we require a null logical type to encode the timestamp
                        null -> {
                            { encoder.encodeLong(value.toMillisOfDay().toLong()) }
                        }

                        "time-micros" -> {
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
                    when (it.logicalType?.name) {
                        "time-millis", null -> {
                            { LocalTime.ofNanoOfDay(decoder.decodeInt() * 1_000_000L) }
                        }

                        else -> null
                    }
                }

                Schema.Type.LONG -> {
                    when (it.logicalType?.name) {
                        null -> {
                            { LocalTime.ofNanoOfDay(decoder.decodeLong() * 1_000_000) }
                        }

                        "time-micros" -> {
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

public object LocalDateTimeSerializer : AvroSerializer<LocalDateTime>() {
    override val descriptor: SerialDescriptor =
        PrimitiveSerialDescriptor("timestamp-millis", PrimitiveKind.LONG).asAvroLogicalType()

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

public object InstantSerializer : AvroSerializer<Instant>() {
    override val descriptor: SerialDescriptor =
        PrimitiveSerialDescriptor("timestamp-millis", PrimitiveKind.LONG).asAvroLogicalType()

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
                    when (it.logicalType?.name) {
                        "timestamp-millis", null -> {
                            { encoder.encodeLong(value.toEpochMilli()) }
                        }

                        "timestamp-micros" -> {
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
                Schema.Type.LONG ->
                    when (it.logicalType?.name) {
                        "timestamp-millis", null -> {
                            { Instant.ofEpochMilli(decoder.decodeLong()) }
                        }

                        "timestamp-micros" -> {
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

public object InstantToMicroSerializer : AvroSerializer<Instant>() {
    override val descriptor: SerialDescriptor =
        PrimitiveSerialDescriptor("timestamp-micros", PrimitiveKind.LONG).asAvroLogicalType()

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
                    when (it.logicalType?.name) {
                        "timestamp-micros", null -> {
                            { encoder.encodeLong(value.toEpochMicros()) }
                        }

                        "timestamp-millis" -> {
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
                Schema.Type.LONG ->
                    when (it.logicalType?.name) {
                        "timestamp-micros", null -> {
                            { Instant.EPOCH.plus(decoder.decodeLong(), ChronoUnit.MICROS) }
                        }

                        "timestamp-millis" -> {
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

private fun Instant.toEpochMicros() = ChronoUnit.MICROS.between(Instant.EPOCH, this)