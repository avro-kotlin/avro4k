@file:OptIn(kotlin.uuid.ExperimentalUuidApi::class, kotlin.time.ExperimentalTime::class)

package com.github.avrokotlin.avro4k.serializer

import com.github.avrokotlin.avro4k.AnyValueDecoder
import com.github.avrokotlin.avro4k.AvroDecoder
import com.github.avrokotlin.avro4k.AvroEncoder
import com.github.avrokotlin.avro4k.AvroFixed
import com.github.avrokotlin.avro4k.decodeResolvingAny
import com.github.avrokotlin.avro4k.internal.UnexpectedDecodeSchemaError
import com.github.avrokotlin.avro4k.internal.copy
import com.github.avrokotlin.avro4k.logicalTypeMismatchError
import com.github.avrokotlin.avro4k.trySelectFixedSchemaForSize
import com.github.avrokotlin.avro4k.trySelectLogicalTypeFromUnion
import com.github.avrokotlin.avro4k.trySelectTypeNameFromUnion
import com.github.avrokotlin.avro4k.unsupportedWriterTypeError
import kotlinx.serialization.SerializationException
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.contextual
import org.apache.avro.LogicalType
import org.apache.avro.Schema
import kotlin.time.Instant
import kotlin.uuid.Uuid

internal val KotlinStdLibSerializersModule: SerializersModule
    get() =
        SerializersModule {
            contextual(KotlinUuidSerializer)
            contextual(KotlinInstantSerializer)
        }

/**
 * Serializes a [kotlin.uuid.Uuid] as a string logical type of `uuid`.
 *
 * Note: it does not check if the schema logical type name is `uuid` as it does not make any conversion.
 */
public object KotlinUuidSerializer : AvroSerializer<Uuid>(Uuid::class.qualifiedName!!) {
    internal const val LOGICAL_TYPE_NAME = "uuid"

    override fun getSchema(context: SchemaSupplierContext): Schema {
        return context.inlinedElements.firstNotNullOfOrNull { element ->
            element.stringable?.createSchema()
                ?: element.fixed?.createSchema(element)
                    ?.copy(logicalType = LogicalType(LOGICAL_TYPE_NAME))
                    ?.also {
                        if (it.fixedSize != 16) {
                            throw SerializationException(
                                "Uuid's @${AvroFixed::class.simpleName} must have bytes size of 16. Got ${it.fixedSize}."
                            )
                        }
                    }
        } ?: Schema.create(Schema.Type.STRING).copy(logicalType = LogicalType(LOGICAL_TYPE_NAME))
    }

    override fun serializeAvro(
        encoder: AvroEncoder,
        value: Uuid,
    ) {
        with(encoder) {
            if (currentWriterSchema.isUnion) {
                trySelectLogicalTypeFromUnion(LOGICAL_TYPE_NAME, Schema.Type.FIXED) ||
                    trySelectTypeNameFromUnion(Schema.Type.STRING) ||
                    trySelectFixedSchemaForSize(16) ||
                    throw unsupportedWriterTypeError(Schema.Type.STRING, Schema.Type.FIXED)
            }
            when (currentWriterSchema.type) {
                Schema.Type.STRING -> encodeString(value.toString())
                Schema.Type.FIXED -> encodeFixed(value.toByteArray())
                else -> throw unsupportedWriterTypeError(Schema.Type.STRING, Schema.Type.FIXED)
            }
        }
    }

    override fun serializeGeneric(
        encoder: Encoder,
        value: Uuid,
    ) {
        encoder.encodeString(value.toString())
    }

    override fun deserializeAvro(decoder: AvroDecoder): Uuid {
        with(decoder) {
            return decodeResolvingAny({
                UnexpectedDecodeSchemaError(
                    "Uuid",
                    Schema.Type.STRING,
                    Schema.Type.FIXED
                )
            }) { schema ->
                when (schema.type) {
                    Schema.Type.STRING -> {
                        AnyValueDecoder { Uuid.parse(decoder.decodeString()) }
                    }

                    Schema.Type.FIXED -> {
                        if (schema.fixedSize == 16) {
                            AnyValueDecoder { Uuid.fromByteArray(decoder.decodeBytes()) }
                        } else {
                            null
                        }
                    }

                    else -> null
                }
            }
        }
    }

    override fun deserializeGeneric(decoder: Decoder): Uuid {
        return Uuid.parse(decoder.decodeString())
    }
}

private const val NANOS_PER_MILLISECOND = 1_000_000L
private const val NANOS_PER_MICROSECOND = 1_000L
private const val MILLIS_PER_SECOND = 1_000L
private const val MICROS_PER_SECOND = 1_000_000L
private const val NANOS_PER_SECOND = 1_000_000_000L

/**
 * Serializes a [kotlin.time.Instant] as a long logical type of `timestamp-millis` by default.
 *
 * Supports the following Avro logical types:
 * - `timestamp-millis` (default)
 * - `timestamp-micros`
 * - `timestamp-nanos`
 *
 * Also supports string type for ISO-8601 representation.
 */
public object KotlinInstantSerializer : AvroSerializer<Instant>(Instant::class.qualifiedName!!) {
    override fun getSchema(context: SchemaSupplierContext): Schema {
        return context.inlinedElements.firstNotNullOfOrNull {
            it.stringable?.createSchema()
        } ?: Schema.create(Schema.Type.LONG).copy(logicalType = LogicalType(LOGICAL_TYPE_NAME_TIMESTAMP_MILLIS))
    }

    override fun serializeAvro(
        encoder: AvroEncoder,
        value: Instant,
    ) {
        with(encoder) {
            if (currentWriterSchema.isUnion) {
                trySelectTypeNameFromUnion(Schema.Type.LONG) ||
                    trySelectTypeNameFromUnion(Schema.Type.STRING) ||
                    throw unsupportedWriterTypeError(Schema.Type.LONG, Schema.Type.STRING)
            }
            when (currentWriterSchema.type) {
                Schema.Type.LONG ->
                    when (currentWriterSchema.logicalType?.name) {
                        LOGICAL_TYPE_NAME_TIMESTAMP_NANOS -> encodeLong(value.toEpochNanos())
                        LOGICAL_TYPE_NAME_TIMESTAMP_MICROS -> encodeLong(value.toEpochMicros())
                        LOGICAL_TYPE_NAME_TIMESTAMP_MILLIS -> encodeLong(value.toEpochMillis())
                        else -> throw logicalTypeMismatchError(LOGICAL_TYPE_NAME_TIMESTAMP_MILLIS, Schema.Type.LONG)
                    }

                Schema.Type.STRING -> encodeString(value.toString())

                else -> throw unsupportedWriterTypeError(Schema.Type.LONG, Schema.Type.STRING)
            }
        }
    }

    override fun serializeGeneric(
        encoder: Encoder,
        value: Instant,
    ) {
        encoder.encodeString(value.toString())
    }

    override fun deserializeAvro(decoder: AvroDecoder): Instant =
        with(decoder) {
            decodeResolvingAny({ UnexpectedDecodeSchemaError("Instant", Schema.Type.LONG) }) {
                when (it.type) {
                    Schema.Type.LONG ->
                        when (it.logicalType?.name) {
                            LOGICAL_TYPE_NAME_TIMESTAMP_MILLIS, null -> {
                                AnyValueDecoder { Instant.fromEpochMilliseconds(decoder.decodeLong()) }
                            }

                            LOGICAL_TYPE_NAME_TIMESTAMP_MICROS -> {
                                AnyValueDecoder { Instant.fromEpochMicros(decoder.decodeLong()) }
                            }

                            LOGICAL_TYPE_NAME_TIMESTAMP_NANOS -> {
                                AnyValueDecoder { Instant.fromEpochNanos(decoder.decodeLong()) }
                            }

                            else -> null
                        }

                    Schema.Type.STRING -> {
                        AnyValueDecoder { Instant.parse(decoder.decodeString()) }
                    }

                    else -> null
                }
            }
        }

    override fun deserializeGeneric(decoder: Decoder): Instant {
        return Instant.parse(decoder.decodeString())
    }

    private fun Instant.toEpochMillis(): Long {
        return toAvroTimestamp(
            MILLIS_PER_SECOND,
            nanosecondsOfSecond.toLong() / NANOS_PER_MILLISECOND,
            LOGICAL_TYPE_NAME_TIMESTAMP_MILLIS
        )
    }

    private fun Instant.toEpochMicros(): Long {
        return toAvroTimestamp(
            MICROS_PER_SECOND,
            nanosecondsOfSecond.toLong() / NANOS_PER_MICROSECOND,
            LOGICAL_TYPE_NAME_TIMESTAMP_MICROS
        )
    }

    private fun Instant.Companion.fromEpochMicros(micros: Long): Instant {
        val seconds = micros / MICROS_PER_SECOND
        val nanos = (micros % MICROS_PER_SECOND) * NANOS_PER_MICROSECOND
        return fromEpochSeconds(seconds, nanos)
    }

    private fun Instant.toEpochNanos(): Long {
        return toAvroTimestamp(NANOS_PER_SECOND, nanosecondsOfSecond.toLong(), LOGICAL_TYPE_NAME_TIMESTAMP_NANOS)
    }

    private fun Instant.toAvroTimestamp(
        unitsPerSecond: Long,
        unitsOfSecond: Long,
        logicalType: String,
    ): Long {
        try {
            return Math.addExact(Math.multiplyExact(epochSeconds, unitsPerSecond), unitsOfSecond)
        } catch (e: ArithmeticException) {
            throw SerializationException("$this is out of range for Avro $logicalType", e)
        }
    }

    private fun Instant.Companion.fromEpochNanos(nanos: Long): Instant {
        val seconds = nanos / NANOS_PER_SECOND
        val nanosOfSecond = nanos % NANOS_PER_SECOND
        return fromEpochSeconds(seconds, nanosOfSecond)
    }
}