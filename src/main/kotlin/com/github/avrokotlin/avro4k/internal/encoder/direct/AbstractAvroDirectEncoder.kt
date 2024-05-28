package com.github.avrokotlin.avro4k.internal.encoder.direct

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroEncoder
import com.github.avrokotlin.avro4k.UnionEncoder
import com.github.avrokotlin.avro4k.encodeResolving
import com.github.avrokotlin.avro4k.internal.BadEncodedValueError
import com.github.avrokotlin.avro4k.internal.isFullNameOrAliasMatch
import com.github.avrokotlin.avro4k.internal.zeroPadded
import kotlinx.serialization.SerializationException
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.descriptors.PolymorphicKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.encoding.AbstractEncoder
import kotlinx.serialization.encoding.CompositeEncoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema
import org.apache.avro.generic.GenericFixed
import java.nio.ByteBuffer

internal class AvroValueDirectEncoder(
    override var currentWriterSchema: Schema,
    avro: Avro,
    binaryEncoder: org.apache.avro.io.Encoder,
) : AbstractAvroDirectEncoder(avro, binaryEncoder)

internal sealed class AbstractAvroDirectEncoder(
    protected val avro: Avro,
    protected val binaryEncoder: org.apache.avro.io.Encoder,
) : AbstractEncoder(), AvroEncoder, UnionEncoder {
    private var selectedUnionIndex: Int = -1

    abstract override var currentWriterSchema: Schema

    override fun encodeInline(descriptor: SerialDescriptor): Encoder = this

    override val serializersModule: SerializersModule
        get() = avro.serializersModule

    override fun <T> encodeSerializableValue(
        serializer: SerializationStrategy<T>,
        value: T,
    ) {
        if (value is ByteArray && currentWriterSchema.isTypeOfBytes()) {
            encodeBytes(value)
        } else {
            super<AbstractEncoder>.encodeSerializableValue(serializer, value)
        }
    }

    private fun Schema.isTypeOfBytes() =
        type == Schema.Type.BYTES ||
            type == Schema.Type.UNION && types.any { it.type == Schema.Type.BYTES }

    override fun beginStructure(descriptor: SerialDescriptor): CompositeEncoder {
        return when (descriptor.kind) {
            StructureKind.CLASS,
            StructureKind.OBJECT,
            ->
                encodeResolving(
                    { BadEncodedValueError(null, currentWriterSchema, Schema.Type.RECORD) }
                ) { schema ->
                    if (schema.type == Schema.Type.RECORD && schema.isFullNameOrAliasMatch(descriptor)) {
                        {
                            val elementDescriptors = avro.recordResolver.resolveFields(schema, descriptor)
                            RecordDirectEncoder(elementDescriptors, schema, avro, binaryEncoder)
                        }
                    } else {
                        null
                    }
                }

            is PolymorphicKind -> PolymorphicDirectEncoder(avro, currentWriterSchema, binaryEncoder)
            else -> throw SerializationException("Unsupported structure kind: $descriptor")
        }
    }

    override fun beginCollection(
        descriptor: SerialDescriptor,
        collectionSize: Int,
    ): CompositeEncoder {
        return when (descriptor.kind) {
            StructureKind.LIST ->
                encodeResolving(
                    { BadEncodedValueError(emptyList<Any?>(), currentWriterSchema, Schema.Type.ARRAY, Schema.Type.BYTES, Schema.Type.FIXED) }
                ) { schema ->
                    when (schema.type) {
                        Schema.Type.ARRAY -> {
                            { ArrayDirectEncoder(schema, collectionSize, avro, binaryEncoder) }
                        }

                        Schema.Type.BYTES -> {
                            { BytesDirectEncoder(avro, binaryEncoder, collectionSize) }
                        }

                        Schema.Type.FIXED -> {
                            { FixedDirectEncoder(schema, collectionSize, avro, binaryEncoder) }
                        }

                        else -> null
                    }
                }

            StructureKind.MAP ->
                encodeResolving(
                    { BadEncodedValueError(emptyMap<String, Any?>(), currentWriterSchema, Schema.Type.MAP) }
                ) { schema ->
                    when (schema.type) {
                        Schema.Type.MAP -> {
                            { MapDirectEncoder(schema, collectionSize, avro, binaryEncoder) }
                        }

                        else -> null
                    }
                }

            else -> throw SerializationException("Unsupported collection kind: $descriptor")
        }
    }

    override fun encodeUnionIndex(index: Int) {
        if (selectedUnionIndex > -1) {
            throw SerializationException("Already selected union index: $selectedUnionIndex, got $index, for selected schema $currentWriterSchema")
        }
        if (currentWriterSchema.type == Schema.Type.UNION) {
            binaryEncoder.writeIndex(index)
            selectedUnionIndex = index
            currentWriterSchema = currentWriterSchema.types[index]
        } else {
            throw SerializationException("Cannot select union index for non-union schema: $currentWriterSchema")
        }
    }

    override fun encodeElement(
        descriptor: SerialDescriptor,
        index: Int,
    ): Boolean {
        selectedUnionIndex = -1
        return true
    }

    override fun encodeNull() {
        encodeResolving(
            { BadEncodedValueError(null, currentWriterSchema, Schema.Type.NULL) }
        ) {
            when (it.type) {
                Schema.Type.NULL -> {
                    { binaryEncoder.writeNull() }
                }

                else -> null
            }
        }
    }

    override fun encodeBytes(value: ByteBuffer) {
        encodeResolving(
            { BadEncodedValueError(value, currentWriterSchema, Schema.Type.BYTES, Schema.Type.STRING) }
        ) {
            when (it.type) {
                Schema.Type.STRING,
                Schema.Type.BYTES,
                -> {
                    { binaryEncoder.writeBytes(value) }
                }

                Schema.Type.FIXED ->
                    if (value.remaining() <= it.fixedSize) {
                        { binaryEncoder.writeFixed(value.array().zeroPadded(it, endPadded = false)) }
                    } else {
                        null
                    }

                else -> null
            }
        }
    }

    override fun encodeBytes(value: ByteArray) {
        encodeResolving(
            { BadEncodedValueError(value, currentWriterSchema, Schema.Type.BYTES, Schema.Type.STRING) }
        ) {
            when (it.type) {
                Schema.Type.STRING,
                Schema.Type.BYTES,
                -> {
                    { binaryEncoder.writeBytes(value) }
                }

                Schema.Type.FIXED ->
                    if (value.size <= it.fixedSize) {
                        { binaryEncoder.writeFixed(value.zeroPadded(it, endPadded = false)) }
                    } else {
                        null
                    }

                else -> null
            }
        }
    }

    override fun encodeFixed(value: GenericFixed) {
        encodeResolving(
            { BadEncodedValueError(value, currentWriterSchema, Schema.Type.FIXED) }
        ) {
            when (it.type) {
                Schema.Type.FIXED ->
                    when (it.fullName) {
                        value.schema.fullName ->
                            when (it.fixedSize) {
                                value.bytes().size -> {
                                    { binaryEncoder.writeFixed(value.bytes()) }
                                }

                                else -> null
                            }

                        else -> null
                    }

                Schema.Type.STRING,
                Schema.Type.BYTES,
                -> {
                    { binaryEncoder.writeBytes(value.bytes()) }
                }

                else -> null
            }
        }
    }

    override fun encodeFixed(value: ByteArray) {
        encodeResolving(
            { BadEncodedValueError(value, currentWriterSchema, Schema.Type.FIXED) }
        ) {
            when (it.type) {
                Schema.Type.FIXED ->
                    when (it.fixedSize) {
                        value.size -> {
                            { binaryEncoder.writeFixed(value) }
                        }

                        else -> null
                    }

                Schema.Type.STRING,
                Schema.Type.BYTES,
                -> {
                    { binaryEncoder.writeBytes(value) }
                }

                else -> null
            }
        }
    }

    override fun encodeEnum(
        enumDescriptor: SerialDescriptor,
        index: Int,
    ) {
        val enumName = enumDescriptor.getElementName(index)
        encodeResolving(
            { BadEncodedValueError(index, currentWriterSchema, Schema.Type.ENUM, Schema.Type.STRING) }
        ) {
            when (it.type) {
                Schema.Type.ENUM ->
                    if (it.isFullNameOrAliasMatch(enumDescriptor)) {
                        { binaryEncoder.writeEnum(it.getEnumOrdinal(enumName)) }
                    } else {
                        null
                    }

                Schema.Type.STRING -> {
                    { binaryEncoder.writeString(enumName) }
                }

                else -> null
            }
        }
    }

    override fun encodeBoolean(value: Boolean) {
        encodeResolving(
            { BadEncodedValueError(value, currentWriterSchema, Schema.Type.BOOLEAN, Schema.Type.STRING) }
        ) {
            when (it.type) {
                Schema.Type.BOOLEAN -> {
                    { binaryEncoder.writeBoolean(value) }
                }

                Schema.Type.STRING -> {
                    { binaryEncoder.writeString(value.toString()) }
                }

                else -> null
            }
        }
    }

    override fun encodeByte(value: Byte) {
        encodeInt(value.toInt())
    }

    override fun encodeShort(value: Short) {
        encodeInt(value.toInt())
    }

    override fun encodeInt(value: Int) {
        encodeResolving(
            { BadEncodedValueError(value, currentWriterSchema, Schema.Type.INT, Schema.Type.LONG, Schema.Type.FLOAT, Schema.Type.DOUBLE, Schema.Type.STRING) }
        ) {
            when (it.type) {
                Schema.Type.INT -> {
                    { binaryEncoder.writeInt(value) }
                }

                Schema.Type.LONG -> {
                    { binaryEncoder.writeLong(value.toLong()) }
                }

                Schema.Type.FLOAT -> {
                    { binaryEncoder.writeFloat(value.toFloat()) }
                }

                Schema.Type.DOUBLE -> {
                    { binaryEncoder.writeDouble(value.toDouble()) }
                }

                Schema.Type.STRING -> {
                    { binaryEncoder.writeString(value.toString()) }
                }

                else -> null
            }
        }
    }

    override fun encodeLong(value: Long) {
        encodeResolving(
            { BadEncodedValueError(value, currentWriterSchema, Schema.Type.LONG, Schema.Type.FLOAT, Schema.Type.DOUBLE, Schema.Type.STRING) }
        ) {
            when (it.type) {
                Schema.Type.LONG -> {
                    { binaryEncoder.writeLong(value) }
                }

                Schema.Type.FLOAT -> {
                    { binaryEncoder.writeFloat(value.toFloat()) }
                }

                Schema.Type.DOUBLE -> {
                    { binaryEncoder.writeDouble(value.toDouble()) }
                }

                Schema.Type.STRING -> {
                    { binaryEncoder.writeString(value.toString()) }
                }

                else -> null
            }
        }
    }

    override fun encodeFloat(value: Float) {
        encodeResolving(
            { BadEncodedValueError(value, currentWriterSchema, Schema.Type.FLOAT, Schema.Type.DOUBLE, Schema.Type.STRING) }
        ) {
            when (it.type) {
                Schema.Type.FLOAT -> {
                    { binaryEncoder.writeFloat(value) }
                }

                Schema.Type.DOUBLE -> {
                    { binaryEncoder.writeDouble(value.toDouble()) }
                }

                Schema.Type.STRING -> {
                    { binaryEncoder.writeString(value.toString()) }
                }

                else -> null
            }
        }
    }

    override fun encodeDouble(value: Double) {
        encodeResolving(
            { BadEncodedValueError(value, currentWriterSchema, Schema.Type.DOUBLE, Schema.Type.STRING) }
        ) {
            when (it.type) {
                Schema.Type.DOUBLE -> {
                    { binaryEncoder.writeDouble(value) }
                }

                Schema.Type.STRING -> {
                    { binaryEncoder.writeString(value.toString()) }
                }

                else -> null
            }
        }
    }

    override fun encodeChar(value: Char) {
        encodeResolving(
            { BadEncodedValueError(value, currentWriterSchema, Schema.Type.INT, Schema.Type.STRING) }
        ) {
            when (it.type) {
                Schema.Type.INT -> {
                    { binaryEncoder.writeInt(value.code) }
                }

                Schema.Type.STRING -> {
                    { binaryEncoder.writeString(value.toString()) }
                }

                else -> null
            }
        }
    }

    override fun encodeString(value: String) {
        encodeResolving(
            { BadEncodedValueError(value, currentWriterSchema, Schema.Type.STRING, Schema.Type.BYTES, Schema.Type.FIXED, Schema.Type.ENUM) }
        ) {
            when (it.type) {
                Schema.Type.STRING -> {
                    { binaryEncoder.writeString(value) }
                }

                Schema.Type.BYTES -> {
                    { binaryEncoder.writeBytes(value.encodeToByteArray()) }
                }

                Schema.Type.FIXED -> {
                    { binaryEncoder.writeFixed(value.encodeToByteArray().zeroPadded(it, endPadded = true)) }
                }

                Schema.Type.ENUM -> {
                    { binaryEncoder.writeEnum(it.getEnumOrdinal(value)) }
                }

                else -> null
            }
        }
    }
}

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