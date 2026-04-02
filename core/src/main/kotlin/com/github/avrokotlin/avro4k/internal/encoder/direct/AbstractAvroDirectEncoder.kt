package com.github.avrokotlin.avro4k.internal.encoder.direct

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.internal.encoder.AbstractAvroEncoder
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.AbstractEncoder
import kotlinx.serialization.encoding.CompositeEncoder
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema
import org.apache.avro.util.Utf8

internal class AvroValueDirectEncoder(
    override var currentWriterSchema: Schema,
    avro: Avro,
    binaryEncoder: org.apache.avro.io.Encoder,
) : AbstractAvroDirectEncoder(avro, binaryEncoder)

internal sealed class AbstractAvroDirectEncoder(
    protected val avro: Avro,
    protected val binaryEncoder: org.apache.avro.io.Encoder,
) : AbstractAvroEncoder() {
    override val serializersModule: SerializersModule
        get() = avro.serializersModule

    override fun getRecordEncoder(descriptor: SerialDescriptor): CompositeEncoder {
        return RecordDirectEncoder(descriptor, currentWriterSchema, avro, binaryEncoder)
    }

    override fun getPolymorphicEncoder(descriptor: SerialDescriptor): CompositeEncoder {
        return PolymorphicDirectEncoder(avro, currentWriterSchema, binaryEncoder)
    }

    override fun getArrayEncoder(
        descriptor: SerialDescriptor,
        collectionSize: Int,
    ): CompositeEncoder {
        return ArrayDirectEncoder(currentWriterSchema, collectionSize, avro, binaryEncoder)
    }

    override fun getMapEncoder(
        descriptor: SerialDescriptor,
        collectionSize: Int,
    ): CompositeEncoder {
        return MapDirectEncoder(currentWriterSchema, collectionSize, avro, binaryEncoder)
    }

    override fun encodeUnionIndexUnchecked(index: Int) {
        binaryEncoder.writeIndex(index)
    }

    override fun encodeNullUnchecked() {
        binaryEncoder.writeNull()
    }

    override fun encodeBytesUnchecked(value: ByteArray) {
        binaryEncoder.writeBytes(value)
    }

    override fun encodeBooleanUnchecked(value: Boolean) {
        binaryEncoder.writeBoolean(value)
    }

    override fun encodeIntUnchecked(value: Int) {
        binaryEncoder.writeInt(value)
    }

    override fun encodeLongUnchecked(value: Long) {
        binaryEncoder.writeLong(value)
    }

    override fun encodeFloatUnchecked(value: Float) {
        binaryEncoder.writeFloat(value)
    }

    override fun encodeDoubleUnchecked(value: Double) {
        binaryEncoder.writeDouble(value)
    }

    override fun encodeStringUnchecked(value: Utf8) {
        binaryEncoder.writeString(value)
    }

    override fun encodeStringUnchecked(value: String) {
        binaryEncoder.writeString(value)
    }

    override fun encodeEnumUnchecked(symbol: String) {
        binaryEncoder.writeEnum(currentWriterSchema.getEnumOrdinalChecked(symbol))
    }

    override fun encodeFixedUnchecked(value: ByteArray) {
        binaryEncoder.writeFixed(value)
    }
}

private fun Schema.getEnumOrdinalChecked(symbol: String): Int {
    return try {
        getEnumOrdinal(symbol)
    } catch (e: Exception) {
        throw SerializationException("Enum symbol $symbol not found in schema $this", e)
    }
}

@OptIn(ExperimentalSerializationApi::class)
internal class PolymorphicDirectEncoder(
    private val avro: Avro,
    private val schema: Schema,
    private val binaryEncoder: org.apache.avro.io.Encoder,
) : AbstractEncoder() {
    override val serializersModule: SerializersModule
        get() = avro.serializersModule

    override fun encodeElement(
        descriptor: SerialDescriptor,
        index: Int,
    ): Boolean {
        // index 0 is the type discriminator, index 1 is the value itself
        // we don't need the type discriminator here
        return index == 1
    }

    override fun <T> encodeSerializableValue(
        serializer: SerializationStrategy<T>,
        value: T,
    ) {
        AvroValueDirectEncoder(schema, avro, binaryEncoder)
            .encodeSerializableValue(serializer, value)
    }
}