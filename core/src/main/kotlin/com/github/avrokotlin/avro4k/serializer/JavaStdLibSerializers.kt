package com.github.avrokotlin.avro4k.serializer

import com.github.avrokotlin.avro4k.AnyValueDecoder
import com.github.avrokotlin.avro4k.AvroDecimal
import com.github.avrokotlin.avro4k.AvroDecoder
import com.github.avrokotlin.avro4k.AvroEncoder
import com.github.avrokotlin.avro4k.AvroFixed
import com.github.avrokotlin.avro4k.decodeResolvingAny
import com.github.avrokotlin.avro4k.internal.AvroSchemaGenerationException
import com.github.avrokotlin.avro4k.internal.UnexpectedDecodeSchemaError
import com.github.avrokotlin.avro4k.internal.copy
import com.github.avrokotlin.avro4k.trySelectFixedSchemaForSize
import com.github.avrokotlin.avro4k.trySelectLogicalTypeFromUnion
import com.github.avrokotlin.avro4k.trySelectTypeNameFromUnion
import com.github.avrokotlin.avro4k.unsupportedWriterTypeError
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.contextual
import org.apache.avro.Conversions
import org.apache.avro.LogicalType
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import java.math.BigDecimal
import java.math.BigInteger
import java.net.URL
import java.nio.ByteBuffer
import java.util.UUID

internal val JavaStdLibSerializersModule: SerializersModule
    get() =
        SerializersModule {
            contextual(URLSerializer)
            contextual(UUIDSerializer)
            contextual(BigIntegerSerializer)
            contextual(BigDecimalSerializer)
        }

public object URLSerializer : KSerializer<URL> {
    override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor(URL::class.qualifiedName!!, PrimitiveKind.STRING)

    override fun serialize(
        encoder: Encoder,
        value: URL,
    ) {
        encoder.encodeString(value.toString())
    }

    override fun deserialize(decoder: Decoder): URL = URL(decoder.decodeString())
}

/**
 * Serializes an [UUID] as a string logical type of `uuid`.
 *
 * Note: it does not check if the schema logical type name is `uuid` as it does not make any conversion.
 */
public object UUIDSerializer : AvroSerializer<UUID>(UUID::class.qualifiedName!!) {
    internal const val LOGICAL_TYPE_NAME = "uuid"

    override fun getSchema(context: SchemaSupplierContext): Schema {
        return context.inlinedElements.firstNotNullOfOrNull { element ->
            element.stringable?.createSchema()
                ?: element.fixed?.createSchema(element)
                    ?.copy(logicalType = LogicalType(LOGICAL_TYPE_NAME))
                    ?.also {
                        if (it.fixedSize != 16) {
                            throw SerializationException(
                                "UUID's @${AvroFixed::class.simpleName} must have bytes size of 16. Got ${it.fixedSize}."
                            )
                        }
                    }
        } ?: Schema.create(Schema.Type.STRING).copy(logicalType = LogicalType(LOGICAL_TYPE_NAME))
    }

    override fun serializeAvro(
        encoder: AvroEncoder,
        value: UUID,
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

    private fun UUID.toByteArray(): ByteArray =
        ByteBuffer.allocate(16)
            .putLong(mostSignificantBits)
            .putLong(leastSignificantBits)
            .array()

    override fun serializeGeneric(
        encoder: Encoder,
        value: UUID,
    ) {
        encoder.encodeString(value.toString())
    }

    override fun deserializeAvro(decoder: AvroDecoder): UUID {
        with(decoder) {
            return decodeResolvingAny({
                UnexpectedDecodeSchemaError(
                    "UUID",
                    Schema.Type.STRING,
                    Schema.Type.FIXED
                )
            }) { schema ->
                when (schema.type) {
                    Schema.Type.STRING -> {
                        AnyValueDecoder { UUID.fromString(decoder.decodeString()) }
                    }

                    Schema.Type.FIXED -> {
                        if (schema.fixedSize == 16) {
                            AnyValueDecoder { parseUuid(decoder.decodeBytes()) }
                        } else {
                            null
                        }
                    }

                    else -> null
                }
            }
        }
    }

    private fun parseUuid(bytes: ByteArray): UUID {
        return ByteBuffer.wrap(bytes).let {
            UUID(
                it.getLong(),
                it.getLong()
            )
        }
    }

    override fun deserializeGeneric(decoder: Decoder): UUID {
        return UUID.fromString(decoder.decodeString())
    }
}

public object BigIntegerSerializer : AvroSerializer<BigInteger>(BigInteger::class.qualifiedName!!) {
    override fun getSchema(context: SchemaSupplierContext): Schema {
        return Schema.create(Schema.Type.STRING)
    }

    override fun serializeAvro(
        encoder: AvroEncoder,
        value: BigInteger,
    ) {
        with(encoder) {
            if (currentWriterSchema.isUnion) {
                trySelectTypeNameFromUnion(Schema.Type.STRING) ||
                    trySelectTypeNameFromUnion(Schema.Type.INT) ||
                    trySelectTypeNameFromUnion(Schema.Type.LONG) ||
                    trySelectTypeNameFromUnion(Schema.Type.FLOAT) ||
                    trySelectTypeNameFromUnion(Schema.Type.DOUBLE) ||
                    throw unsupportedWriterTypeError(Schema.Type.STRING, Schema.Type.INT, Schema.Type.LONG, Schema.Type.FLOAT, Schema.Type.DOUBLE)
            }
            when (currentWriterSchema.type) {
                Schema.Type.STRING -> encodeString(value.toString())
                Schema.Type.INT -> encodeInt(value.intValueExact())
                Schema.Type.LONG -> encodeLong(value.longValueExact())
                Schema.Type.FLOAT -> encodeFloat(value.toFloat())
                Schema.Type.DOUBLE -> encodeDouble(value.toDouble())
                else -> throw unsupportedWriterTypeError(Schema.Type.STRING, Schema.Type.INT, Schema.Type.LONG, Schema.Type.FLOAT, Schema.Type.DOUBLE)
            }
        }
    }

    override fun serializeGeneric(
        encoder: Encoder,
        value: BigInteger,
    ) {
        encoder.encodeString(value.toString())
    }

    override fun deserializeAvro(decoder: AvroDecoder): BigInteger {
        with(decoder) {
            return decodeResolvingAny({
                UnexpectedDecodeSchemaError(
                    "BigInteger",
                    Schema.Type.STRING,
                    Schema.Type.INT,
                    Schema.Type.LONG,
                    Schema.Type.FLOAT,
                    Schema.Type.DOUBLE
                )
            }) { schema ->
                when (schema.type) {
                    Schema.Type.STRING -> {
                        AnyValueDecoder { decoder.decodeString().toBigInteger() }
                    }

                    Schema.Type.INT -> {
                        AnyValueDecoder { decoder.decodeInt().toBigInteger() }
                    }

                    Schema.Type.LONG -> {
                        AnyValueDecoder { decoder.decodeLong().toBigInteger() }
                    }

                    Schema.Type.FLOAT -> {
                        AnyValueDecoder { decoder.decodeFloat().toBigDecimal().toBigIntegerExact() }
                    }

                    Schema.Type.DOUBLE -> {
                        AnyValueDecoder { decoder.decodeDouble().toBigDecimal().toBigIntegerExact() }
                    }

                    else -> null
                }
            }
        }
    }

    override fun deserializeGeneric(decoder: Decoder): BigInteger {
        return decoder.decodeString().toBigInteger()
    }
}

public object BigDecimalSerializer : AvroSerializer<BigDecimal>(BigDecimal::class.qualifiedName!!) {
    internal const val LOGICAL_TYPE_NAME = "decimal"
    private val converter = Conversions.DecimalConversion()

    override fun getSchema(context: SchemaSupplierContext): Schema {
        val logicalType = context.inlinedElements.firstNotNullOfOrNull { it.decimal }?.logicalType

        fun nonNullLogicalType(): LogicalTypes.Decimal {
            if (logicalType == null) {
                throw AvroSchemaGenerationException("BigDecimal requires @${AvroDecimal::class.qualifiedName} to works with 'fixed' or 'bytes' schema types.")
            }
            return logicalType
        }
        return context.inlinedElements.firstNotNullOfOrNull {
            it.stringable?.createSchema() ?: it.fixed?.createSchema(it)?.copy(logicalType = nonNullLogicalType())
        } ?: Schema.create(Schema.Type.BYTES).copy(logicalType = nonNullLogicalType())
    }

    override fun serializeAvro(
        encoder: AvroEncoder,
        value: BigDecimal,
    ) {
        with(encoder) {
            if (currentWriterSchema.isUnion) {
                trySelectLogicalTypeFromUnion(converter.logicalTypeName, Schema.Type.BYTES, Schema.Type.FIXED) ||
                    trySelectTypeNameFromUnion(Schema.Type.STRING) ||
                    trySelectTypeNameFromUnion(Schema.Type.INT) ||
                    trySelectTypeNameFromUnion(Schema.Type.LONG) ||
                    trySelectTypeNameFromUnion(Schema.Type.FLOAT) ||
                    trySelectTypeNameFromUnion(Schema.Type.DOUBLE) ||
                    throw unsupportedWriterTypeError(
                        Schema.Type.BYTES,
                        Schema.Type.FIXED,
                        Schema.Type.STRING,
                        Schema.Type.INT,
                        Schema.Type.LONG,
                        Schema.Type.FLOAT,
                        Schema.Type.DOUBLE
                    )
            }
            when (currentWriterSchema.type) {
                Schema.Type.BYTES -> encodeBytes(converter.toBytes(value, currentWriterSchema, currentWriterSchema.logicalType).array())
                Schema.Type.FIXED -> encodeFixed(converter.toFixed(value, currentWriterSchema, currentWriterSchema.logicalType).bytes())
                Schema.Type.STRING -> encodeString(value.toString())
                Schema.Type.INT -> encodeInt(value.intValueExact())
                Schema.Type.LONG -> encodeLong(value.longValueExact())
                Schema.Type.FLOAT -> encodeFloat(value.toFloat())
                Schema.Type.DOUBLE -> encodeDouble(value.toDouble())
                else -> throw unsupportedWriterTypeError(
                    Schema.Type.BYTES,
                    Schema.Type.FIXED,
                    Schema.Type.STRING,
                    Schema.Type.INT,
                    Schema.Type.LONG,
                    Schema.Type.FLOAT,
                    Schema.Type.DOUBLE
                )
            }
        }
    }

    override fun serializeGeneric(
        encoder: Encoder,
        value: BigDecimal,
    ) {
        encoder.encodeString(value.toString())
    }

    override fun deserializeAvro(decoder: AvroDecoder): BigDecimal {
        with(decoder) {
            return decodeResolvingAny({
                UnexpectedDecodeSchemaError(
                    "BigDecimal",
                    Schema.Type.STRING,
                    Schema.Type.BYTES,
                    Schema.Type.FIXED
                )
            }) { schema ->
                when (schema.type) {
                    Schema.Type.STRING -> {
                        AnyValueDecoder { decoder.decodeString().toBigDecimal() }
                    }

                    Schema.Type.BYTES ->
                        when (schema.logicalType) {
                            is LogicalTypes.Decimal -> {
                                AnyValueDecoder { converter.fromBytes(ByteBuffer.wrap(decoder.decodeBytes()), schema, schema.logicalType) }
                            }

                            else -> null
                        }

                    Schema.Type.FIXED ->
                        when (schema.logicalType) {
                            is LogicalTypes.Decimal -> {
                                AnyValueDecoder { converter.fromFixed(decoder.decodeFixed(), schema, schema.logicalType) }
                            }

                            else -> null
                        }

                    else -> null
                }
            }
        }
    }

    override fun deserializeGeneric(decoder: Decoder): BigDecimal {
        return decoder.decodeString().toBigDecimal()
    }

    private val AvroDecimal.logicalType: LogicalTypes.Decimal
        get() {
            return LogicalTypes.decimal(precision, scale)
        }
}