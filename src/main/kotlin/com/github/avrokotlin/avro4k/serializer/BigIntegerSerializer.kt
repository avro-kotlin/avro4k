package com.github.avrokotlin.avro4k.serializer

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
import java.math.BigInteger

public object BigIntegerSerializer : AvroSerializer<BigInteger>() {
    override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor(BigInteger::class.qualifiedName!!, PrimitiveKind.STRING)

    override fun serializeAvro(
        encoder: AvroEncoder,
        value: BigInteger,
    ) {
        encoder.encodeResolvingUnion({
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
        return decoder.decodeResolvingUnion({
            with(decoder) {
                BadDecodedValueError(
                    decoder.decodeValue(),
                    decoder.currentWriterSchema,
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
                    { decoder.decodeString().toBigInteger() }
                }
                Schema.Type.INT -> {
                    { decoder.decodeInt().toBigInteger() }
                }
                Schema.Type.LONG -> {
                    { decoder.decodeLong().toBigInteger() }
                }
                Schema.Type.FLOAT -> {
                    { decoder.decodeFloat().toBigDecimal().toBigIntegerExact() }
                }
                Schema.Type.DOUBLE -> {
                    { decoder.decodeDouble().toBigDecimal().toBigIntegerExact() }
                }
                else -> null
            }
        }
    }

    override fun deserializeGeneric(decoder: Decoder): BigInteger {
        return decoder.decodeString().toBigInteger()
    }
}