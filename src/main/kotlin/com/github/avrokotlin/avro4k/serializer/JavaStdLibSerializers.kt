package com.github.avrokotlin.avro4k.serializer

import com.github.avrokotlin.avro4k.AnyValueDecoder
import com.github.avrokotlin.avro4k.AvroDecimal
import com.github.avrokotlin.avro4k.AvroDecoder
import com.github.avrokotlin.avro4k.AvroEncoder
import com.github.avrokotlin.avro4k.decodeResolvingAny
import com.github.avrokotlin.avro4k.encodeResolving
import com.github.avrokotlin.avro4k.internal.AvroSchemaGenerationException
import com.github.avrokotlin.avro4k.internal.BadEncodedValueError
import com.github.avrokotlin.avro4k.internal.UnexpectedDecodeSchemaError
import com.github.avrokotlin.avro4k.internal.copy
import kotlinx.serialization.KSerializer
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
import java.util.*

public val JavaStdLibSerializersModule: SerializersModule =
    SerializersModule {
        contextual(URLSerializer)
        contextual(UUIDSerializer)
        contextual(BigIntegerSerializer)
        contextual(BigDecimalSerializer)
        contextual(ByteBufferSerializer)
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
    override fun getSchema(context: SchemaSupplierContext): Schema {
        return Schema.create(Schema.Type.STRING).copy(logicalType = LogicalType("uuid"))
    }

    override fun serializeAvro(
        encoder: AvroEncoder,
        value: UUID,
    ) {
        serializeGeneric(encoder, value)
    }

    override fun deserializeAvro(decoder: AvroDecoder): UUID {
        return deserializeGeneric(decoder)
    }

    override fun serializeGeneric(
        encoder: Encoder,
        value: UUID,
    ) {
        encoder.encodeString(value.toString())
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
        encoder.encodeResolving({
            with(encoder) {
                BadEncodedValueError(
                    value,
                    encoder.currentWriterSchema,
                    Schema.Type.STRING,
                    Schema.Type.INT,
                    Schema.Type.LONG,
                    Schema.Type.FLOAT,
                    Schema.Type.DOUBLE
                )
            }
        }) { schema ->
            when (schema.type) {
                Schema.Type.STRING -> {
                    { encoder.encodeString(value.toString()) }
                }

                Schema.Type.INT -> {
                    { encoder.encodeInt(value.intValueExact()) }
                }

                Schema.Type.LONG -> {
                    { encoder.encodeLong(value.longValueExact()) }
                }

                Schema.Type.FLOAT -> {
                    { encoder.encodeFloat(value.toFloat()) }
                }

                Schema.Type.DOUBLE -> {
                    { encoder.encodeDouble(value.toDouble()) }
                }

                else -> null
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
        encoder.encodeResolving({
            with(encoder) {
                BadEncodedValueError(
                    value,
                    encoder.currentWriterSchema,
                    Schema.Type.BYTES,
                    Schema.Type.FIXED,
                    Schema.Type.STRING,
                    Schema.Type.INT,
                    Schema.Type.LONG,
                    Schema.Type.FLOAT,
                    Schema.Type.DOUBLE
                )
            }
        }) { schema ->
            when (schema.type) {
                Schema.Type.BYTES ->
                    when (schema.logicalType) {
                        is LogicalTypes.Decimal -> {
                            { encoder.encodeBytes(converter.toBytes(value, schema, schema.logicalType)) }
                        }

                        else -> null
                    }

                Schema.Type.FIXED ->
                    when (schema.logicalType) {
                        is LogicalTypes.Decimal -> {
                            { encoder.encodeFixed(converter.toFixed(value, schema, schema.logicalType)) }
                        }

                        else -> null
                    }

                Schema.Type.STRING -> {
                    { encoder.encodeString(value.toString()) }
                }

                Schema.Type.INT -> {
                    { encoder.encodeInt(value.intValueExact()) }
                }

                Schema.Type.LONG -> {
                    { encoder.encodeLong(value.longValueExact()) }
                }

                Schema.Type.FLOAT -> {
                    { encoder.encodeFloat(value.toFloat()) }
                }

                Schema.Type.DOUBLE -> {
                    { encoder.encodeDouble(value.toDouble()) }
                }

                else -> null
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

/**
 * Delegates the serialization of a [ByteBuffer] to [AvroEncoder.encodeBytes] and [AvroDecoder.decodeBytes] as all the compatible types
 * for the current writer schema are specifically handled here.
 */
public object ByteBufferSerializer : AvroSerializer<ByteBuffer>(ByteBuffer::class.qualifiedName!!) {
    override fun getSchema(context: SchemaSupplierContext): Schema {
        return Schema.create(Schema.Type.BYTES)
    }

    override fun serializeAvro(
        encoder: AvroEncoder,
        value: ByteBuffer,
    ) {
        if (value.hasArray() && value.arrayOffset() == 0 && value.array().size == value.remaining()) {
            encoder.encodeBytes(value.array())
        } else {
            val bytes = ByteArray(value.remaining())
            value.get(bytes)
            encoder.encodeBytes(bytes)
        }
    }

    override fun deserializeAvro(decoder: AvroDecoder): ByteBuffer {
        return ByteBuffer.wrap(decoder.decodeBytes())
    }
}