package com.github.avrokotlin.avro4k.serializer

import com.github.avrokotlin.avro4k.AnyValueDecoder
import com.github.avrokotlin.avro4k.AvroDecoder
import com.github.avrokotlin.avro4k.AvroEncoder
import com.github.avrokotlin.avro4k.decodeResolvingAny
import com.github.avrokotlin.avro4k.ensureFixedSize
import com.github.avrokotlin.avro4k.fullNameOrAliasMismatchError
import com.github.avrokotlin.avro4k.internal.UnexpectedDecodeSchemaError
import com.github.avrokotlin.avro4k.internal.isFullNameOrAliasMatch
import com.github.avrokotlin.avro4k.trySelectLogicalTypeFromUnion
import com.github.avrokotlin.avro4k.trySelectNamedSchema
import com.github.avrokotlin.avro4k.trySelectTypeNameFromUnion
import com.github.avrokotlin.avro4k.unsupportedWriterTypeError
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerializationException
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import org.apache.avro.LogicalType
import org.apache.avro.Schema
import org.intellij.lang.annotations.Language
import java.nio.ByteBuffer
import java.nio.ByteOrder

/**
 * Represents a duration in months, days and milliseconds.
 *
 * This is the exact representation of the Avro `duration` logical type.
 *
 * [avro spec](https://avro.apache.org/docs/1.11.1/specification/#duration)
 */
@SerialName("time.Duration")
@Serializable(with = AvroDurationSerializer::class)
@ExperimentalSerializationApi
public data class AvroDuration(
    val months: UInt,
    val days: UInt,
    val millis: UInt,
) {
    override fun toString(): String {
        if (months == 0u && days == 0u && millis == 0u) {
            return "PT0S"
        }
        return buildString {
            append("P")
            if (months != 0u) {
                append("${months}M")
            }
            if (days != 0u) {
                append("${days}D")
            }
            if (millis != 0u) {
                append("T")
                append(millis / 1000u)
                val millisPart = millis % 1000u
                if (millisPart != 0u) {
                    append('.')
                    append(millisPart)
                }
                append("S")
            }
        }
    }

    public companion object {
        @JvmStatic
        @Language("RegExp")
        private fun part(
            name: Char,
            @Language("RegExp") digitsRegex: String = "",
        ): String {
            val digitsPart = if (digitsRegex.isNotEmpty()) "(?:[.,]($digitsRegex))?" else ""
            return "(?:\\+?([0-9]+)$digitsPart$name)?"
        }

        @JvmStatic
        private val PATTERN: Regex =
            buildString {
                append("P")
                append(part('Y'))
                append(part('M'))
                append(part('W'))
                append(part('D'))
                append("(?:T")
                append(part('H'))
                append(part('M'))
                append(part('S', digitsRegex = "[0-9]{0,3}"))
                append(")?")
            }.toRegex(RegexOption.IGNORE_CASE)

        @JvmStatic
        @Throws(AvroDurationParseException::class)
        public fun tryParse(value: String): AvroDuration? {
            val match = PATTERN.matchEntire(value) ?: return null
            val (years, months, weeks, days, hours, minutes, seconds, millis) = match.destructured
            return AvroDuration(
                months = years * 12u + months.toUIntOrZero(),
                days = weeks * 7u + days.toUIntOrZero(),
                millis = hours * 60u * 60u * 1000u + minutes * 60u * 1000u + seconds * 1000u + millis.toUIntOrZero()
            )
        }

        private operator fun String.times(other: UInt): UInt {
            return toUIntOrNull()?.times(other) ?: 0u
        }

        private fun String.toUIntOrZero(): UInt {
            return toUIntOrNull() ?: 0u
        }

        @JvmStatic
        public fun parse(value: String): AvroDuration {
            return tryParse(value) ?: throw AvroDurationParseException(value)
        }
    }
}

@ExperimentalSerializationApi
public class AvroDurationParseException(value: String) : SerializationException("Unable to parse duration: $value")

internal object AvroDurationSerializer : AvroSerializer<AvroDuration>(AvroDuration::class.qualifiedName!!) {
    internal const val LOGICAL_TYPE_NAME = "duration"
    private const val DURATION_BYTES = 12
    private const val DEFAULT_DURATION_FULL_NAME = "time.Duration"
    internal val DURATION_SCHEMA =
        Schema.createFixed(DEFAULT_DURATION_FULL_NAME, "A 12-byte byte array encoding a duration in months, days and milliseconds.", null, DURATION_BYTES).also {
            LogicalType(LOGICAL_TYPE_NAME).addToSchema(it)
        }

    override fun getSchema(context: SchemaSupplierContext): Schema {
        return context.inlinedElements.firstNotNullOfOrNull {
            it.stringable?.createSchema()
        } ?: DURATION_SCHEMA
    }

    override fun serializeAvro(
        encoder: AvroEncoder,
        value: AvroDuration,
    ) {
        with(encoder) {
            if (currentWriterSchema.isUnion) {
                trySelectNamedSchema(DEFAULT_DURATION_FULL_NAME) ||
                    trySelectLogicalTypeFromUnion(LOGICAL_TYPE_NAME, Schema.Type.FIXED) ||
                    trySelectTypeNameFromUnion(Schema.Type.STRING) ||
                    throw unsupportedWriterTypeError(Schema.Type.FIXED, Schema.Type.STRING)
            }
            when (currentWriterSchema.type) {
                Schema.Type.FIXED ->
                    if (currentWriterSchema.logicalType?.name == LOGICAL_TYPE_NAME || currentWriterSchema.isFullNameOrAliasMatch(DEFAULT_DURATION_FULL_NAME, ::emptySet)) {
                        encodeFixed(ensureFixedSize(encodeDuration(value)))
                    } else {
                        throw fullNameOrAliasMismatchError(DEFAULT_DURATION_FULL_NAME, emptySet())
                    }

                Schema.Type.STRING -> encodeString(value.toString())
                else -> throw unsupportedWriterTypeError(Schema.Type.FIXED, Schema.Type.STRING)
            }
        }
    }

    override fun deserializeAvro(decoder: AvroDecoder): AvroDuration {
        return with(decoder) {
            decodeResolvingAny({ UnexpectedDecodeSchemaError(AvroDuration::class.qualifiedName!!, Schema.Type.FIXED, Schema.Type.STRING) }) {
                when (it.type) {
                    Schema.Type.FIXED -> {
                        if (it.logicalType?.name == LOGICAL_TYPE_NAME && it.fixedSize == DURATION_BYTES) {
                            AnyValueDecoder { decodeDuration(decodeFixed().bytes()) }
                        } else {
                            null
                        }
                    }

                    Schema.Type.STRING -> {
                        AnyValueDecoder { AvroDuration.parse(decodeString()) }
                    }

                    else -> throw SerializationException("Expected the duration logical type to be of type fixed or string, but had ${it.type}")
                }
            }
        }
    }

    private fun encodeDuration(value: AvroDuration): ByteArray {
        val buffer = ByteBuffer.allocate(DURATION_BYTES).order(ByteOrder.LITTLE_ENDIAN)
        buffer.putInt(value.months.toInt())
        buffer.putInt(value.days.toInt())
        buffer.putInt(value.millis.toInt())
        return buffer.array()
    }

    private fun decodeDuration(bytes: ByteArray): AvroDuration {
        val buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
        return AvroDuration(
            months = buffer.getInt().toUInt(),
            days = buffer.getInt().toUInt(),
            millis = buffer.getInt().toUInt()
        )
    }

    override fun serializeGeneric(
        encoder: Encoder,
        value: AvroDuration,
    ) {
        encoder.encodeString(value.toString())
    }

    override fun deserializeGeneric(decoder: Decoder): AvroDuration {
        return AvroDuration.parse(decoder.decodeString())
    }
}