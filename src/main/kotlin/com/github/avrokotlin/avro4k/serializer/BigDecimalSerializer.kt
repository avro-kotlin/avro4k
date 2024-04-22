package com.github.avrokotlin.avro4k.serializer

import com.github.avrokotlin.avro4k.AnnotatedLocation
import com.github.avrokotlin.avro4k.AvroDecimal
import com.github.avrokotlin.avro4k.AvroLogicalType
import com.github.avrokotlin.avro4k.AvroLogicalTypeSupplier
import com.github.avrokotlin.avro4k.decoder.ExtendedDecoder
import com.github.avrokotlin.avro4k.encoder.ExtendedEncoder
import com.github.avrokotlin.avro4k.schema.findElementAnnotation
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
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

    override val descriptor =
        buildByteArraySerialDescriptor(
            BigDecimal::class.qualifiedName!!,
            AvroLogicalType(BigDecimalSerializer::class)
        )

    override fun encodeAvroValue(
        schema: Schema,
        encoder: ExtendedEncoder,
        obj: BigDecimal,
    ) = encodeBigDecimal(schema, encoder, obj)

    override fun decodeAvroValue(
        schema: Schema,
        decoder: ExtendedDecoder,
    ) = decodeBigDecimal(decoder, schema)

    private val AvroDecimal.logicalType: LogicalType
        get() {
            return LogicalTypes.decimal(precision, scale)
        }
}

object BigDecimalAsStringSerializer : AvroSerializer<BigDecimal>() {
    override val descriptor = PrimitiveSerialDescriptor(BigDecimal::class.qualifiedName!!, PrimitiveKind.STRING)

    override fun encodeAvroValue(
        schema: Schema,
        encoder: ExtendedEncoder,
        obj: BigDecimal,
    ) = encodeBigDecimal(schema, encoder, obj)

    override fun decodeAvroValue(
        schema: Schema,
        decoder: ExtendedDecoder,
    ) = decodeBigDecimal(decoder, schema)
}

private fun encodeBigDecimal(
    schema: Schema,
    encoder: ExtendedEncoder,
    value: BigDecimal,
) {
    when (schema.type) {
        Schema.Type.STRING -> encoder.encodeString(value.toString())
        Schema.Type.BYTES -> {
            encoder.encodeByteArray(converter.toBytes(value, schema, schema.getDecimalLogicalType()))
        }

        Schema.Type.FIXED -> {
            encoder.encodeFixed(converter.toFixed(value, schema, schema.getDecimalLogicalType()))
        }

        Schema.Type.INT -> encoder.encodeInt(value.intValueExact())
        Schema.Type.LONG -> encoder.encodeLong(value.longValueExact())
        Schema.Type.FLOAT -> encoder.encodeFloat(value.toFloat())
        Schema.Type.DOUBLE -> encoder.encodeDouble(value.toDouble())

        else -> throw SerializationException("Cannot encode BigDecimal as ${schema.type}")
    }
}

private fun decodeBigDecimal(
    decoder: ExtendedDecoder,
    schema: Schema,
): BigDecimal =
    // TODO we should use the schema instead of this generic decodeAny()
    when (val v = decoder.decodeAny()) {
        is CharSequence -> BigDecimal(v.toString())
        is ByteArray -> converter.fromBytes(ByteBuffer.wrap(v), schema, schema.getDecimalLogicalType())
        is ByteBuffer -> converter.fromBytes(v, schema, schema.getDecimalLogicalType())
        is GenericFixed -> converter.fromFixed(v, schema, schema.getDecimalLogicalType())
        else -> throw SerializationException("Unsupported BigDecimal type [$v]")
    }

private fun Schema.getDecimalLogicalType(): LogicalTypes.Decimal {
    val l = logicalType
    return when (l) {
        is LogicalTypes.Decimal -> l
        else -> throw SerializationException("Expected to find a decimal logical type for BigDecimal but found $l")
    }
}