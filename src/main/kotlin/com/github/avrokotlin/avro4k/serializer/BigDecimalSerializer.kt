package com.github.avrokotlin.avro4k.serializer

import com.github.avrokotlin.avro4k.AnnotatedLocation
import com.github.avrokotlin.avro4k.AvroDecimal
import com.github.avrokotlin.avro4k.AvroLogicalType
import com.github.avrokotlin.avro4k.AvroLogicalTypeSupplier
import com.github.avrokotlin.avro4k.decoder.AvroDecoder
import com.github.avrokotlin.avro4k.encoder.AvroEncoder
import com.github.avrokotlin.avro4k.encoder.SchemaTypeMatcher
import com.github.avrokotlin.avro4k.encoder.encodeValueResolved
import com.github.avrokotlin.avro4k.schema.findElementAnnotation
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.descriptors.buildSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import org.apache.avro.Conversions
import org.apache.avro.LogicalType
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import org.apache.avro.generic.GenericFixed
import java.math.BigDecimal
import java.nio.ByteBuffer

private val converter = Conversions.DecimalConversion()
private val defaultAnnotation = AvroDecimal()

object BigDecimalSerializer : AvroSerializer<BigDecimal>(), AvroLogicalTypeSupplier {
    override fun getLogicalType(inlinedStack: List<AnnotatedLocation>): LogicalType {
        return inlinedStack.firstNotNullOfOrNull {
            it.descriptor.findElementAnnotation<AvroDecimal>(it.elementIndex ?: return@firstNotNullOfOrNull null)?.logicalType
        } ?: defaultAnnotation.logicalType
    }

    @OptIn(InternalSerializationApi::class)
    override val descriptor =
        buildSerialDescriptor(BigDecimal::class.qualifiedName!!, StructureKind.LIST) {
            element("item", buildSerialDescriptor("item", PrimitiveKind.BYTE))
            this.annotations = listOf(AvroLogicalType(BigDecimalSerializer::class))
        }

    override fun serializeAvro(
        encoder: AvroEncoder,
        value: BigDecimal,
    ) = encodeBigDecimal(encoder, value)

    override fun serializeGeneric(
        encoder: Encoder,
        value: BigDecimal,
    ) = encoder.encodeString(value.toString())

    override fun deserializeAvro(decoder: AvroDecoder) = decodeBigDecimal(decoder)

    override fun deserializeGeneric(decoder: Decoder): BigDecimal {
        return decoder.decodeString().toBigDecimal()
    }

    private val AvroDecimal.logicalType: LogicalType
        get() {
            return LogicalTypes.decimal(precision, scale)
        }
}

object BigDecimalAsStringSerializer : AvroSerializer<BigDecimal>() {
    override val descriptor = PrimitiveSerialDescriptor(BigDecimal::class.qualifiedName!!, PrimitiveKind.STRING)

    override fun serializeAvro(
        encoder: AvroEncoder,
        value: BigDecimal,
    ) = encodeBigDecimal(encoder, value)

    override fun serializeGeneric(
        encoder: Encoder,
        value: BigDecimal,
    ) = encoder.encodeString(value.toString())

    override fun deserializeAvro(decoder: AvroDecoder) = decodeBigDecimal(decoder)

    override fun deserializeGeneric(decoder: Decoder): BigDecimal {
        return decoder.decodeString().toBigDecimal()
    }
}

private fun encodeBigDecimal(
    encoder: AvroEncoder,
    value: BigDecimal,
) {
    encoder.encodeValueResolved<BigDecimal>(
        SchemaTypeMatcher.Scalar.BYTES to { converter.toBytes(value, it, it.getDecimalLogicalType()) },
        SchemaTypeMatcher.Named.FirstFixed to { converter.toFixed(value, it, it.getDecimalLogicalType()) },
        SchemaTypeMatcher.Scalar.STRING to { value.toString() },
        SchemaTypeMatcher.Scalar.INT to { value.intValueExact() },
        SchemaTypeMatcher.Scalar.LONG to { value.longValueExact() },
        SchemaTypeMatcher.Scalar.FLOAT to { value.toFloat() },
        SchemaTypeMatcher.Scalar.DOUBLE to { value.toDouble() }
    )
}

private fun decodeBigDecimal(decoder: AvroDecoder): BigDecimal =
    when (val v = decoder.decodeValue()) {
        is CharSequence -> BigDecimal(v.toString())
        is ByteArray -> converter.fromBytes(ByteBuffer.wrap(v), decoder.currentWriterSchema, decoder.currentWriterSchema.getDecimalLogicalType())
        is ByteBuffer -> converter.fromBytes(v, decoder.currentWriterSchema, decoder.currentWriterSchema.getDecimalLogicalType())
        is GenericFixed -> converter.fromFixed(v, decoder.currentWriterSchema, decoder.currentWriterSchema.getDecimalLogicalType())
        else -> throw SerializationException("Unsupported BigDecimal type [$v]")
    }

private fun Schema.getDecimalLogicalType(): LogicalTypes.Decimal {
    return when (val l = logicalType) {
        is LogicalTypes.Decimal -> l
        else -> throw SerializationException("Expected to find a decimal logical type for BigDecimal but found $l")
    }
}