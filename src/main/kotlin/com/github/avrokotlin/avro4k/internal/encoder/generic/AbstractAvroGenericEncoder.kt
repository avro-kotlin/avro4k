package com.github.avrokotlin.avro4k.internal.encoder.generic

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.internal.encoder.AbstractAvroEncoder
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.CompositeEncoder
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import java.nio.ByteBuffer

internal abstract class AbstractAvroGenericEncoder : AbstractAvroEncoder() {
    abstract val avro: Avro

    abstract override fun encodeValue(value: Any)

    override val serializersModule: SerializersModule
        get() = avro.serializersModule

    override fun getRecordEncoder(descriptor: SerialDescriptor): CompositeEncoder {
        return RecordGenericEncoder(descriptor, currentWriterSchema, avro) { encodeValue(it) }
    }

    override fun getPolymorphicEncoder(descriptor: SerialDescriptor): CompositeEncoder {
        return PolymorphicEncoder(avro, currentWriterSchema) { encodeValue(it) }
    }

    override fun getArrayEncoder(
        descriptor: SerialDescriptor,
        collectionSize: Int,
    ): CompositeEncoder {
        return ArrayGenericEncoder(avro, collectionSize, currentWriterSchema) { encodeValue(it) }
    }

    override fun getMapEncoder(
        descriptor: SerialDescriptor,
        collectionSize: Int,
    ): CompositeEncoder {
        return MapGenericEncoder(avro, collectionSize, currentWriterSchema) { encodeValue(it) }
    }

    override fun encodeBytesUnchecked(value: ByteArray) {
        encodeValue(ByteBuffer.wrap(value))
    }

    override fun encodeBooleanUnchecked(value: Boolean) {
        encodeValue(value)
    }

    override fun encodeStringUnchecked(value: Utf8) {
        encodeValue(value)
    }

    override fun encodeUnionIndexInternal(index: Int) {
        // nothing to do
    }

    override fun encodeFixedUnchecked(value: ByteArray) {
        encodeValue(GenericData.Fixed(currentWriterSchema, value))
    }

    override fun encodeIntUnchecked(value: Int) {
        encodeValue(value)
    }

    override fun encodeLongUnchecked(value: Long) {
        encodeValue(value)
    }

    override fun encodeFloatUnchecked(value: Float) {
        encodeValue(value)
    }

    override fun encodeDoubleUnchecked(value: Double) {
        encodeValue(value)
    }

    override fun encodeEnumUnchecked(symbol: String) {
        encodeValue(GenericData.EnumSymbol(currentWriterSchema, symbol))
    }
}