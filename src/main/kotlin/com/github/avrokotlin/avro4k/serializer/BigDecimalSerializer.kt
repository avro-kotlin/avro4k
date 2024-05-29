package com.github.avrokotlin.avro4k.serializer

import com.github.avrokotlin.avro4k.AnyValueDecoder
import com.github.avrokotlin.avro4k.AvroDecimal
import com.github.avrokotlin.avro4k.AvroDecoder
import com.github.avrokotlin.avro4k.AvroEncoder
import com.github.avrokotlin.avro4k.decodeResolvingAny
import com.github.avrokotlin.avro4k.encodeResolving
import com.github.avrokotlin.avro4k.internal.BadEncodedValueError
import com.github.avrokotlin.avro4k.internal.UnexpectedDecodeSchemaError
import com.github.avrokotlin.avro4k.internal.asAvroLogicalType
import com.github.avrokotlin.avro4k.internal.findElementAnnotation
import kotlinx.serialization.builtins.ByteArraySerializer
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import org.apache.avro.Conversions
import org.apache.avro.LogicalType
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import java.math.BigDecimal
import java.nio.ByteBuffer

private val converter = Conversions.DecimalConversion()
private val defaultAnnotation = AvroDecimal()

public object BigDecimalSerializer : AvroSerializer<BigDecimal>() {
    override val descriptor: SerialDescriptor =
        SerialDescriptor(BigDecimal::class.qualifiedName!!, ByteArraySerializer().descriptor)
            .asAvroLogicalType { inlinedStack ->
                inlinedStack.firstNotNullOfOrNull {
                    it.descriptor.findElementAnnotation<AvroDecimal>(it.elementIndex ?: return@firstNotNullOfOrNull null)?.logicalType
                } ?: defaultAnnotation.logicalType
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

    private val AvroDecimal.logicalType: LogicalType
        get() {
            return LogicalTypes.decimal(precision, scale)
        }
}

public object BigDecimalAsStringSerializer : AvroSerializer<BigDecimal>() {
    override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor(BigDecimal::class.qualifiedName!!, PrimitiveKind.STRING)

    override fun serializeAvro(
        encoder: AvroEncoder,
        value: BigDecimal,
    ) {
        BigDecimalSerializer.serializeAvro(encoder, value)
    }

    override fun serializeGeneric(
        encoder: Encoder,
        value: BigDecimal,
    ) {
        encoder.encodeString(value.toString())
    }

    override fun deserializeAvro(decoder: AvroDecoder): BigDecimal {
        return BigDecimalSerializer.deserializeAvro(decoder)
    }

    override fun deserializeGeneric(decoder: Decoder): BigDecimal {
        return decoder.decodeString().toBigDecimal()
    }
}