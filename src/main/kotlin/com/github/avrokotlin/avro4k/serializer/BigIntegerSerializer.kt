package com.github.avrokotlin.avro4k.serializer

import com.github.avrokotlin.avro4k.decoder.ExtendedDecoder
import com.github.avrokotlin.avro4k.encoder.ExtendedEncoder
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import org.apache.avro.Schema
import java.math.BigInteger

object BigIntegerSerializer : AvroSerializer<BigInteger>() {
    override val descriptor = PrimitiveSerialDescriptor(BigInteger::class.qualifiedName!!, PrimitiveKind.STRING)

    override fun encodeAvroValue(
        schema: Schema,
        encoder: ExtendedEncoder,
        obj: BigInteger,
    ) = when (schema.type) {
        Schema.Type.STRING -> encoder.encodeString(obj.toString())
        Schema.Type.INT -> encoder.encodeInt(obj.intValueExact())
        Schema.Type.LONG -> encoder.encodeLong(obj.longValueExact())
        Schema.Type.FLOAT -> encoder.encodeFloat(obj.toFloat())
        Schema.Type.DOUBLE -> encoder.encodeDouble(obj.toDouble())

        else -> throw UnsupportedOperationException("Unsupported schema type: $schema")
    }

    override fun decodeAvroValue(
        schema: Schema,
        decoder: ExtendedDecoder,
    ): BigInteger =
        when (schema.type) {
            Schema.Type.STRING -> BigInteger(decoder.decodeString())
            Schema.Type.INT -> BigInteger.valueOf(decoder.decodeInt().toLong())
            Schema.Type.LONG -> BigInteger.valueOf(decoder.decodeLong())
            Schema.Type.FLOAT -> BigInteger.valueOf(decoder.decodeFloat().toLong())
            Schema.Type.DOUBLE -> BigInteger.valueOf(decoder.decodeDouble().toLong())

            else -> throw UnsupportedOperationException("Unsupported schema type for BigInteger: $schema")
        }
}