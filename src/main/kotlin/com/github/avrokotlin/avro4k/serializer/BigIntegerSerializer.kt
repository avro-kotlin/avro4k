package com.github.avrokotlin.avro4k.serializer

import com.github.avrokotlin.avro4k.decoder.AvroDecoder
import com.github.avrokotlin.avro4k.encoder.AvroEncoder
import com.github.avrokotlin.avro4k.encoder.SchemaTypeMatcher
import com.github.avrokotlin.avro4k.encoder.encodeValueResolved
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import org.apache.avro.Schema
import java.math.BigInteger

public object BigIntegerSerializer : AvroSerializer<BigInteger>() {
    override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor(BigInteger::class.qualifiedName!!, PrimitiveKind.STRING)

    override fun serializeAvro(
        encoder: AvroEncoder,
        value: BigInteger,
    ) {
        encoder.encodeValueResolved<BigInteger>(
            SchemaTypeMatcher.Scalar.STRING to { value.toString() },
            SchemaTypeMatcher.Scalar.INT to { value.intValueExact() },
            SchemaTypeMatcher.Scalar.LONG to { value.longValueExact() },
            SchemaTypeMatcher.Scalar.FLOAT to { value.toFloat() },
            SchemaTypeMatcher.Scalar.DOUBLE to { value.toDouble() }
        )
    }

    override fun serializeGeneric(
        encoder: Encoder,
        value: BigInteger,
    ) {
        encoder.encodeString(value.toString())
    }

    override fun deserializeAvro(decoder: AvroDecoder): BigInteger =
        when (decoder.currentWriterSchema.type) {
            Schema.Type.STRING -> BigInteger(decoder.decodeString())
            Schema.Type.INT -> BigInteger.valueOf(decoder.decodeInt().toLong())
            Schema.Type.LONG -> BigInteger.valueOf(decoder.decodeLong())
            Schema.Type.FLOAT -> BigInteger.valueOf(decoder.decodeFloat().toLong())
            Schema.Type.DOUBLE -> BigInteger.valueOf(decoder.decodeDouble().toLong())
            else -> throw UnsupportedOperationException("Unsupported schema type for BigInteger: ${decoder.currentWriterSchema}")
        }

    override fun deserializeGeneric(decoder: Decoder): BigInteger {
        return decoder.decodeString().toBigInteger()
    }
}