package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.internal.BadEncodedValueError
import com.github.avrokotlin.avro4k.internal.nonNullSerialName
import com.github.avrokotlin.avro4k.internal.toIntExact
import com.github.avrokotlin.avro4k.internal.zeroPadded
import kotlinx.serialization.SerializationException
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.descriptors.PolymorphicKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.encoding.AbstractEncoder
import kotlinx.serialization.encoding.CompositeEncoder
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericFixed
import java.nio.ByteBuffer

internal abstract class AbstractAvroEncoder : AbstractEncoder(), AvroEncoder, UnionEncoder {
    abstract val avro: Avro

    abstract override fun encodeValue(value: Any)

    abstract override fun encodeNull()

    override fun encodeElement(
        descriptor: SerialDescriptor,
        index: Int,
    ): Boolean {
        selectedUnionIndex = null
        return true
    }

    private var selectedUnionIndex: Int? = null

    override fun selectUnionIndex(index: Int) {
        if (selectedUnionIndex != null) {
            throw SerializationException("Already selected union index: $selectedUnionIndex, got $index, for selected schema $currentWriterSchema")
        }
        if (currentWriterSchema.type == Schema.Type.UNION) {
            selectedUnionIndex = index
            currentWriterSchema = currentWriterSchema.types[index]
        } else {
            throw SerializationException("Cannot select union index for non-union schema: $currentWriterSchema")
        }
    }

    override val serializersModule: SerializersModule
        get() = avro.serializersModule

    override fun <T> encodeSerializableValue(
        serializer: SerializationStrategy<T>,
        value: T,
    ) {
        if (currentWriterSchema.type == Schema.Type.BYTES ||
            currentWriterSchema.type == Schema.Type.UNION && currentWriterSchema.types.any { it.type == Schema.Type.BYTES }
        ) {
            when (value) {
                is ByteArray -> encodeBytes(value)
                is ByteBuffer -> encodeBytes(value)
                else -> super<AbstractEncoder>.encodeSerializableValue(serializer, value)
            }
        } else {
            super<AbstractEncoder>.encodeSerializableValue(serializer, value)
        }
    }

    override fun beginStructure(descriptor: SerialDescriptor): CompositeEncoder {
        return when (descriptor.kind) {
            StructureKind.CLASS,
            StructureKind.OBJECT,
            ->
                encodeResolvingUnion(
                    { BadEncodedValueError(null, currentWriterSchema, Schema.Type.RECORD) }
                ) { schema ->
                    if (schema.type == Schema.Type.RECORD &&
                        (schema.fullName == descriptor.nonNullSerialName || schema.aliases.any { it == descriptor.nonNullSerialName })
                    ) {
                        { RecordEncoder(avro, descriptor, schema) { encodeValue(it) } }
                    } else {
                        null
                    }
                }

            is PolymorphicKind ->
                PolymorphicEncoder(avro, currentWriterSchema) {
                    encodeValue(it)
                }

            else -> throw SerializationException("Unsupported structure kind: $descriptor")
        }
    }

    override fun beginCollection(
        descriptor: SerialDescriptor,
        collectionSize: Int,
    ): CompositeEncoder {
        return when (descriptor.kind) {
            StructureKind.LIST ->
                encodeResolvingUnion(
                    { BadEncodedValueError(emptyList<Any?>(), currentWriterSchema, Schema.Type.ARRAY, Schema.Type.BYTES, Schema.Type.FIXED) }
                ) { schema ->
                    when (schema.type) {
                        Schema.Type.ARRAY -> {
                            { ArrayEncoder(avro, collectionSize, schema) { encodeValue(it) } }
                        }

                        Schema.Type.BYTES -> {
                            { BytesEncoder(avro, collectionSize) { encodeValue(it) } }
                        }

                        Schema.Type.FIXED -> {
                            { FixedEncoder(avro, collectionSize, schema) { encodeValue(it) } }
                        }

                        else -> null
                    }
                }

            StructureKind.MAP ->
                encodeResolvingUnion(
                    { BadEncodedValueError(emptyMap<String, Any?>(), currentWriterSchema, Schema.Type.MAP) }
                ) { schema ->
                    when (schema.type) {
                        Schema.Type.MAP -> {
                            { MapEncoder(avro, collectionSize, schema) { encodeValue(it) } }
                        }

                        else -> null
                    }
                }

            else -> throw SerializationException("Unsupported collection kind: $descriptor")
        }
    }

    override fun encodeBytes(value: ByteBuffer) {
        encodeResolvingUnion(
            { BadEncodedValueError(value, currentWriterSchema, Schema.Type.STRING, Schema.Type.BYTES, Schema.Type.FIXED) }
        ) { schema ->
            when (schema.type) {
                Schema.Type.BYTES -> {
                    { encodeValue(value) }
                }

                Schema.Type.FIXED -> {
                    { encodeValue(value.array().toPaddedGenericFixed(schema, endPadded = false)) }
                }

                Schema.Type.STRING -> {
                    { encodeValue(value.array().decodeToString()) }
                }

                else -> null
            }
        }
    }

    override fun encodeBytes(value: ByteArray) {
        encodeResolvingUnion(
            { BadEncodedValueError(value, currentWriterSchema, Schema.Type.STRING, Schema.Type.BYTES, Schema.Type.FIXED) }
        ) { schema ->
            when (schema.type) {
                Schema.Type.BYTES -> {
                    { encodeValue(ByteBuffer.wrap(value)) }
                }

                Schema.Type.FIXED -> {
                    { encodeValue(value.toPaddedGenericFixed(schema, endPadded = false)) }
                }

                Schema.Type.STRING -> {
                    { encodeValue(value.decodeToString()) }
                }

                else -> null
            }
        }
    }

    override fun encodeFixed(value: GenericFixed) {
        encodeResolvingUnion(
            { BadEncodedValueError(value, currentWriterSchema, Schema.Type.STRING, Schema.Type.BYTES, Schema.Type.FIXED) }
        ) { schema ->
            when (schema.type) {
                Schema.Type.FIXED ->
                    when (schema.fullName) {
                        value.schema.fullName -> {
                            { encodeValue(value) }
                        }

                        else -> null
                    }

                Schema.Type.BYTES -> {
                    { encodeValue(ByteBuffer.wrap(value.bytes())) }
                }

                Schema.Type.STRING -> {
                    { encodeValue(value.bytes().decodeToString()) }
                }

                else -> null
            }
        }
    }

    override fun encodeFixed(value: ByteArray) {
        encodeResolvingUnion(
            { BadEncodedValueError(value, currentWriterSchema, Schema.Type.STRING, Schema.Type.BYTES, Schema.Type.FIXED) }
        ) { schema ->
            when (schema.type) {
                Schema.Type.FIXED -> {
                    { encodeValue(value.toPaddedGenericFixed(schema, endPadded = false)) }
                }

                Schema.Type.BYTES -> {
                    { encodeValue(ByteBuffer.wrap(value)) }
                }

                Schema.Type.STRING -> {
                    { encodeValue(value.decodeToString()) }
                }

                else -> null
            }
        }
    }

    override fun encodeBoolean(value: Boolean) {
        encodeResolvingUnion(
            { BadEncodedValueError(value, currentWriterSchema, Schema.Type.BOOLEAN, Schema.Type.STRING) }
        ) { schema ->
            when (schema.type) {
                Schema.Type.BOOLEAN -> {
                    { encodeValue(value) }
                }

                Schema.Type.STRING -> {
                    { encodeValue(value.toString()) }
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
        encodeResolvingUnion(
            { BadEncodedValueError(value, currentWriterSchema, Schema.Type.LONG, Schema.Type.INT, Schema.Type.FLOAT, Schema.Type.DOUBLE, Schema.Type.STRING) }
        ) { schema ->
            when (schema.type) {
                Schema.Type.INT -> {
                    { encodeValue(value) }
                }

                Schema.Type.LONG -> {
                    { encodeValue(value.toLong()) }
                }

                Schema.Type.FLOAT -> {
                    { encodeValue(value.toFloat()) }
                }

                Schema.Type.DOUBLE -> {
                    { encodeValue(value.toDouble()) }
                }

                Schema.Type.STRING -> {
                    { encodeValue(value.toString()) }
                }

                else -> null
            }
        }
    }

    override fun encodeLong(value: Long) {
        encodeResolvingUnion(
            { BadEncodedValueError(value, currentWriterSchema, Schema.Type.LONG, Schema.Type.INT, Schema.Type.FLOAT, Schema.Type.DOUBLE, Schema.Type.STRING) }
        ) { schema ->
            when (schema.type) {
                Schema.Type.LONG -> {
                    { encodeValue(value) }
                }

                Schema.Type.INT -> {
                    { encodeValue(value.toIntExact()) }
                }

                Schema.Type.FLOAT -> {
                    { encodeValue(value.toFloat()) }
                }

                Schema.Type.DOUBLE -> {
                    { encodeValue(value.toDouble()) }
                }

                Schema.Type.STRING -> {
                    { encodeValue(value.toString()) }
                }

                else -> null
            }
        }
    }

    override fun encodeFloat(value: Float) {
        encodeResolvingUnion(
            { BadEncodedValueError(value, currentWriterSchema, Schema.Type.STRING, Schema.Type.DOUBLE, Schema.Type.FLOAT) }
        ) { schema ->
            when (schema.type) {
                Schema.Type.FLOAT -> {
                    { encodeValue(value) }
                }

                Schema.Type.DOUBLE -> {
                    { encodeValue(value.toDouble()) }
                }

                Schema.Type.STRING -> {
                    { encodeValue(value.toString()) }
                }

                else -> null
            }
        }
    }

    override fun encodeDouble(value: Double) {
        encodeResolvingUnion(
            { BadEncodedValueError(value, currentWriterSchema, Schema.Type.STRING, Schema.Type.DOUBLE) }
        ) { schema ->
            when (schema.type) {
                Schema.Type.DOUBLE -> {
                    { encodeValue(value) }
                }

                Schema.Type.STRING -> {
                    { encodeValue(value.toString()) }
                }

                else -> null
            }
        }
    }

    override fun encodeChar(value: Char) {
        encodeResolvingUnion(
            { BadEncodedValueError(value, currentWriterSchema, Schema.Type.INT, Schema.Type.STRING) }
        ) { schema ->
            when (schema.type) {
                Schema.Type.INT -> {
                    { encodeValue(value.code) }
                }

                Schema.Type.STRING -> {
                    { encodeValue(value.toString()) }
                }

                else -> null
            }
        }
    }

    override fun encodeString(value: String) {
        encodeResolvingUnion(
            { BadEncodedValueError(value, currentWriterSchema, Schema.Type.STRING, Schema.Type.BYTES, Schema.Type.FIXED, Schema.Type.ENUM) }
        ) { schema ->
            when (schema.type) {
                Schema.Type.STRING -> {
                    { encodeValue(value) }
                }

                Schema.Type.BYTES -> {
                    { encodeValue(value.encodeToByteArray()) }
                }

                Schema.Type.FIXED -> {
                    { encodeValue(value.encodeToByteArray().toPaddedGenericFixed(schema, endPadded = true)) }
                }

                Schema.Type.ENUM -> {
                    { encodeValue(GenericData.EnumSymbol(schema, value)) }
                }

                else -> null
            }
        }
    }

    override fun encodeEnum(
        enumDescriptor: SerialDescriptor,
        index: Int,
    ) {
        /*
        We allow enums as ENUM (must match the descriptor's full name), STRING or UNION.
        For UNION, we look for an enum with the descriptor's full name, otherwise a string.
         */
        val value = enumDescriptor.getElementName(index)

        encodeResolvingUnion(
            { BadEncodedValueError(value, currentWriterSchema, Schema.Type.STRING, Schema.Type.ENUM) }
        ) { schema ->
            when (schema.type) {
                Schema.Type.STRING -> {
                    { encodeValue(value) }
                }

                Schema.Type.ENUM ->
                    when (schema.fullName) {
                        enumDescriptor.nonNullSerialName -> {
                            { encodeValue(GenericData.EnumSymbol(schema, value)) }
                        }

                        else ->
                            schema.aliases.firstOrNull { it == enumDescriptor.nonNullSerialName }?.let {
                                { encodeValue(GenericData.EnumSymbol(schema, value)) }
                            }
                    }

                else -> null
            }
        }
    }
}

private fun ByteArray.toPaddedGenericFixed(
    schema: Schema,
    endPadded: Boolean,
): GenericFixed {
    return GenericData.Fixed(
        schema,
        zeroPadded(schema, endPadded = endPadded)
    )
}