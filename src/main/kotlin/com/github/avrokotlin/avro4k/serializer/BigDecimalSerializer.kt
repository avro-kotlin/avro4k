package com.github.avrokotlin.avro4k.serializer

import com.github.avrokotlin.avro4k.AnnotatedLocation
import com.github.avrokotlin.avro4k.AvroDecimal
import com.github.avrokotlin.avro4k.AvroLogicalType
import com.github.avrokotlin.avro4k.AvroLogicalTypeSupplier
import com.github.avrokotlin.avro4k.decoder.AvroDecoder
import com.github.avrokotlin.avro4k.decoder.decodeResolvingUnion
import com.github.avrokotlin.avro4k.encoder.AvroEncoder
import com.github.avrokotlin.avro4k.encoder.encodeResolvingUnion
import com.github.avrokotlin.avro4k.internal.BadDecodedValueError
import com.github.avrokotlin.avro4k.internal.BadEncodedValueError
import com.github.avrokotlin.avro4k.internal.findElementAnnotation
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.descriptors.buildSerialDescriptor
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

public object BigDecimalSerializer : AvroSerializer<BigDecimal>(), AvroLogicalTypeSupplier {
    override fun getLogicalType(inlinedStack: List<AnnotatedLocation>): LogicalType {
        return inlinedStack.firstNotNullOfOrNull {
            it.descriptor.findElementAnnotation<AvroDecimal>(it.elementIndex ?: return@firstNotNullOfOrNull null)?.logicalType
        } ?: defaultAnnotation.logicalType
    }

    @OptIn(InternalSerializationApi::class)
    override val descriptor: SerialDescriptor =
        buildSerialDescriptor(BigDecimal::class.qualifiedName!!, StructureKind.LIST) {
            element("item", buildSerialDescriptor("item", PrimitiveKind.BYTE))
            this.annotations = listOf(AvroLogicalType(BigDecimalSerializer::class))
        }

    override fun serializeAvro(
        encoder: AvroEncoder,
        value: BigDecimal,
    ) {
        encoder.encodeResolvingUnion({
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
        return decoder.decodeResolvingUnion({
            with(decoder) {
                BadDecodedValueError(
                    decoder.decodeValue(),
                    decoder.currentWriterSchema,
                    Schema.Type.STRING,
                    Schema.Type.BYTES,
                    Schema.Type.FIXED
                )
            }
        }) { schema ->
            when (schema.type) {
                Schema.Type.STRING -> {
                    { decoder.decodeString().toBigDecimal() }
                }

                Schema.Type.BYTES -> when (schema.logicalType) {
                    is LogicalTypes.Decimal -> {
                        { converter.fromBytes(ByteBuffer.wrap(decoder.decodeBytes()), schema, schema.logicalType) }
                    }

                    else -> null
                }

                Schema.Type.FIXED -> when (schema.logicalType) {
                    is LogicalTypes.Decimal -> {
                        { converter.fromFixed(decoder.decodeFixed(), schema, schema.logicalType) }
                    }

                    else -> null
                }

                else -> null
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