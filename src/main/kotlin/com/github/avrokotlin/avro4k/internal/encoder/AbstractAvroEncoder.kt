package com.github.avrokotlin.avro4k.internal.encoder

import com.github.avrokotlin.avro4k.AvroEncoder
import com.github.avrokotlin.avro4k.ensureFixedSize
import com.github.avrokotlin.avro4k.fullNameOrAliasMismatchError
import com.github.avrokotlin.avro4k.internal.SerializerLocatorMiddleware
import com.github.avrokotlin.avro4k.internal.aliases
import com.github.avrokotlin.avro4k.internal.isFullNameOrAliasMatch
import com.github.avrokotlin.avro4k.internal.nonNullSerialName
import com.github.avrokotlin.avro4k.internal.toFloatExact
import com.github.avrokotlin.avro4k.internal.toIntExact
import com.github.avrokotlin.avro4k.namedSchemaNotFoundInUnionError
import com.github.avrokotlin.avro4k.trySelectEnumSchemaForSymbol
import com.github.avrokotlin.avro4k.trySelectFixedSchemaForSize
import com.github.avrokotlin.avro4k.trySelectNamedSchema
import com.github.avrokotlin.avro4k.trySelectSingleNonNullTypeFromUnion
import com.github.avrokotlin.avro4k.trySelectTypeFromUnion
import com.github.avrokotlin.avro4k.unsupportedWriterTypeError
import kotlinx.serialization.SerializationException
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.descriptors.PolymorphicKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.encoding.AbstractEncoder
import kotlinx.serialization.encoding.CompositeEncoder
import org.apache.avro.Schema
import org.apache.avro.util.Utf8

internal abstract class AbstractAvroEncoder : AbstractEncoder(), AvroEncoder {
    private var selectedUnionIndex: Int = -1

    abstract override var currentWriterSchema: Schema

    abstract fun getRecordEncoder(descriptor: SerialDescriptor): CompositeEncoder

    abstract fun getPolymorphicEncoder(descriptor: SerialDescriptor): CompositeEncoder

    abstract fun getMapEncoder(
        descriptor: SerialDescriptor,
        collectionSize: Int,
    ): CompositeEncoder

    abstract fun getArrayEncoder(
        descriptor: SerialDescriptor,
        collectionSize: Int,
    ): CompositeEncoder

    abstract fun encodeUnionIndexUnchecked(index: Int)

    abstract fun encodeNullUnchecked()

    abstract fun encodeBooleanUnchecked(value: Boolean)

    abstract fun encodeIntUnchecked(value: Int)

    abstract fun encodeLongUnchecked(value: Long)

    abstract fun encodeFloatUnchecked(value: Float)

    abstract fun encodeDoubleUnchecked(value: Double)

    abstract fun encodeStringUnchecked(value: Utf8)

    abstract fun encodeStringUnchecked(value: String)

    abstract fun encodeBytesUnchecked(value: ByteArray)

    abstract fun encodeFixedUnchecked(value: ByteArray)

    abstract fun encodeEnumUnchecked(symbol: String)

    override fun <T> encodeSerializableValue(
        serializer: SerializationStrategy<T>,
        value: T,
    ) {
        SerializerLocatorMiddleware.apply(serializer)
            .serialize(this, value)
    }

    override fun beginStructure(descriptor: SerialDescriptor): CompositeEncoder {
        return when (descriptor.kind) {
            StructureKind.CLASS,
            StructureKind.OBJECT,
            -> {
                val nameChecked: Boolean
                if (currentWriterSchema.isUnion && !trySelectSingleNonNullTypeFromUnion()) {
                    trySelectNamedSchema(descriptor).also { nameChecked = it } ||
                        throw namedSchemaNotFoundInUnionError(descriptor.nonNullSerialName, descriptor.aliases)
                } else {
                    nameChecked = false
                }
                when (currentWriterSchema.type) {
                    Schema.Type.RECORD -> {
                        if (nameChecked || currentWriterSchema.isFullNameOrAliasMatch(descriptor)) {
                            getRecordEncoder(descriptor)
                        } else {
                            throw fullNameOrAliasMismatchError(descriptor.nonNullSerialName, descriptor.aliases)
                        }
                    }

                    else -> throw unsupportedWriterTypeError(Schema.Type.RECORD)
                }
            }

            is PolymorphicKind -> getPolymorphicEncoder(descriptor)
            else -> throw SerializationException("Unsupported structure kind: $descriptor")
        }
    }

    override fun beginCollection(
        descriptor: SerialDescriptor,
        collectionSize: Int,
    ): CompositeEncoder {
        return when (descriptor.kind) {
            StructureKind.LIST -> {
                if (currentWriterSchema.isUnion && !trySelectSingleNonNullTypeFromUnion()) {
                    trySelectTypeFromUnion(Schema.Type.ARRAY) || throw unsupportedWriterTypeError(Schema.Type.ARRAY)
                }
                when (currentWriterSchema.type) {
                    Schema.Type.ARRAY -> getArrayEncoder(descriptor, collectionSize)
                    else -> throw unsupportedWriterTypeError(Schema.Type.ARRAY)
                }
            }

            StructureKind.MAP -> {
                if (currentWriterSchema.isUnion && !trySelectSingleNonNullTypeFromUnion()) {
                    trySelectTypeFromUnion(Schema.Type.MAP) || throw unsupportedWriterTypeError(Schema.Type.MAP)
                }
                when (currentWriterSchema.type) {
                    Schema.Type.MAP -> getMapEncoder(descriptor, collectionSize)
                    else -> throw unsupportedWriterTypeError(Schema.Type.MAP)
                }
            }

            else -> throw SerializationException("Unsupported collection kind: $descriptor")
        }
    }

    override fun encodeUnionIndex(index: Int) {
        if (selectedUnionIndex > -1) {
            throw SerializationException("Already selected union index: $selectedUnionIndex, got $index, for selected schema $currentWriterSchema")
        }
        currentWriterSchema = currentWriterSchema.types[index]
        encodeUnionIndexUnchecked(index)
        selectedUnionIndex = index
    }

    override fun encodeElement(
        descriptor: SerialDescriptor,
        index: Int,
    ): Boolean {
        selectedUnionIndex = -1
        return true
    }

    override fun encodeNull() {
        if (currentWriterSchema.isUnion) {
            // Generally, null types are the first or last in the union
            if (currentWriterSchema.types.first().type == Schema.Type.NULL) {
                encodeUnionIndex(0)
            } else if (currentWriterSchema.types.last().type == Schema.Type.NULL) {
                encodeUnionIndex(currentWriterSchema.types.size - 1)
            } else {
                val nullIndex =
                    currentWriterSchema.getIndexNamed(Schema.Type.NULL.getName())
                        ?: throw SerializationException("Cannot encode null value for non-nullable schema: $currentWriterSchema")
                encodeUnionIndex(nullIndex)
            }
        } else if (currentWriterSchema.type != Schema.Type.NULL) {
            throw SerializationException("Cannot encode null value for non-null schema: $currentWriterSchema")
        }
        encodeNullUnchecked()
    }

    override fun encodeBytes(value: ByteArray) {
        if (currentWriterSchema.isUnion && !trySelectSingleNonNullTypeFromUnion()) {
            trySelectTypeFromUnion(Schema.Type.BYTES, Schema.Type.STRING) ||
                trySelectFixedSchemaForSize(value.size) ||
                throw unsupportedWriterTypeError(Schema.Type.FIXED, Schema.Type.BYTES, Schema.Type.STRING)
        }
        when (currentWriterSchema.type) {
            Schema.Type.BYTES -> encodeBytesUnchecked(value)
            Schema.Type.STRING -> encodeStringUnchecked(Utf8(value))
            Schema.Type.FIXED -> encodeFixedUnchecked(ensureFixedSize(value))
            else -> throw unsupportedWriterTypeError(Schema.Type.BYTES, Schema.Type.STRING, Schema.Type.FIXED)
        }
    }

    override fun encodeFixed(value: ByteArray) {
        if (currentWriterSchema.isUnion && !trySelectSingleNonNullTypeFromUnion()) {
            trySelectFixedSchemaForSize(value.size) ||
                trySelectTypeFromUnion(Schema.Type.BYTES, Schema.Type.STRING) ||
                throw unsupportedWriterTypeError(Schema.Type.FIXED, Schema.Type.BYTES, Schema.Type.STRING)
        }
        when (currentWriterSchema.type) {
            Schema.Type.FIXED -> encodeFixedUnchecked(ensureFixedSize(value))
            Schema.Type.BYTES -> encodeBytesUnchecked(value)
            Schema.Type.STRING -> encodeStringUnchecked(Utf8(value))
            else -> throw unsupportedWriterTypeError(Schema.Type.FIXED, Schema.Type.BYTES, Schema.Type.STRING)
        }
    }

    override fun encodeEnum(
        enumDescriptor: SerialDescriptor,
        index: Int,
    ) {
        val nameChecked: Boolean
        if (currentWriterSchema.isUnion && !trySelectSingleNonNullTypeFromUnion()) {
            trySelectNamedSchema(enumDescriptor).also { nameChecked = it } ||
                trySelectTypeFromUnion(Schema.Type.STRING) ||
                throw namedSchemaNotFoundInUnionError(
                    enumDescriptor.nonNullSerialName,
                    enumDescriptor.aliases,
                    Schema.Type.STRING
                )
        } else {
            nameChecked = false
        }
        val enumName = enumDescriptor.getElementName(index)
        when (currentWriterSchema.type) {
            Schema.Type.ENUM ->
                if (nameChecked || currentWriterSchema.isFullNameOrAliasMatch(enumDescriptor)) {
                    encodeEnumUnchecked(enumName)
                } else {
                    throw fullNameOrAliasMismatchError(enumDescriptor.nonNullSerialName, enumDescriptor.aliases)
                }

            Schema.Type.STRING -> encodeStringUnchecked(enumName)
            else -> throw unsupportedWriterTypeError(Schema.Type.ENUM, Schema.Type.STRING)
        }
    }

    override fun encodeBoolean(value: Boolean) {
        if (currentWriterSchema.isUnion && !trySelectSingleNonNullTypeFromUnion()) {
            trySelectTypeFromUnion(Schema.Type.BOOLEAN, Schema.Type.STRING) ||
                throw unsupportedWriterTypeError(Schema.Type.BOOLEAN, Schema.Type.STRING)
        }
        when (currentWriterSchema.type) {
            Schema.Type.BOOLEAN -> encodeBooleanUnchecked(value)
            Schema.Type.STRING -> encodeStringUnchecked(value.toString())
            else -> throw unsupportedWriterTypeError(Schema.Type.BOOLEAN, Schema.Type.STRING)
        }
    }

    override fun encodeByte(value: Byte) {
        encodeInt(value.toInt())
    }

    override fun encodeShort(value: Short) {
        encodeInt(value.toInt())
    }

    override fun encodeInt(value: Int) {
        if (currentWriterSchema.isUnion && !trySelectSingleNonNullTypeFromUnion()) {
            trySelectTypeFromUnion(Schema.Type.INT, Schema.Type.LONG, Schema.Type.STRING) ||
                throw unsupportedWriterTypeError(Schema.Type.INT, Schema.Type.LONG, Schema.Type.STRING)
        }
        when (currentWriterSchema.type) {
            Schema.Type.INT -> encodeIntUnchecked(value)
            Schema.Type.LONG -> encodeLongUnchecked(value.toLong())
            Schema.Type.STRING -> encodeStringUnchecked(value.toString())
            else -> throw unsupportedWriterTypeError(Schema.Type.INT, Schema.Type.LONG, Schema.Type.STRING)
        }
    }

    override fun encodeLong(value: Long) {
        if (currentWriterSchema.isUnion && !trySelectSingleNonNullTypeFromUnion()) {
            trySelectTypeFromUnion(Schema.Type.LONG, Schema.Type.INT, Schema.Type.STRING) ||
                throw unsupportedWriterTypeError(Schema.Type.LONG, Schema.Type.INT, Schema.Type.STRING)
        }
        when (currentWriterSchema.type) {
            Schema.Type.INT -> encodeIntUnchecked(value.toIntExact())
            Schema.Type.LONG -> encodeLongUnchecked(value)
            Schema.Type.STRING -> encodeStringUnchecked(value.toString())
            else -> throw unsupportedWriterTypeError(Schema.Type.INT, Schema.Type.LONG, Schema.Type.STRING)
        }
    }

    override fun encodeFloat(value: Float) {
        if (currentWriterSchema.isUnion && !trySelectSingleNonNullTypeFromUnion()) {
            trySelectTypeFromUnion(Schema.Type.FLOAT, Schema.Type.DOUBLE, Schema.Type.STRING) ||
                throw unsupportedWriterTypeError(Schema.Type.FLOAT, Schema.Type.DOUBLE, Schema.Type.STRING)
        }
        when (currentWriterSchema.type) {
            Schema.Type.FLOAT -> encodeFloatUnchecked(value)
            Schema.Type.DOUBLE -> encodeDoubleUnchecked(value.toDouble())
            Schema.Type.STRING -> encodeStringUnchecked(value.toString())
            else -> throw unsupportedWriterTypeError(Schema.Type.FLOAT, Schema.Type.DOUBLE, Schema.Type.STRING)
        }
    }

    override fun encodeDouble(value: Double) {
        if (currentWriterSchema.isUnion && !trySelectSingleNonNullTypeFromUnion()) {
            trySelectTypeFromUnion(Schema.Type.DOUBLE, Schema.Type.FLOAT, Schema.Type.STRING) ||
                throw unsupportedWriterTypeError(Schema.Type.DOUBLE, Schema.Type.FLOAT, Schema.Type.STRING)
        }
        when (currentWriterSchema.type) {
            Schema.Type.FLOAT -> encodeFloatUnchecked(value.toFloatExact())
            Schema.Type.DOUBLE -> encodeDoubleUnchecked(value)
            Schema.Type.STRING -> encodeStringUnchecked(value.toString())
            else -> throw unsupportedWriterTypeError(Schema.Type.DOUBLE, Schema.Type.FLOAT, Schema.Type.STRING)
        }
    }

    override fun encodeChar(value: Char) {
        if (currentWriterSchema.isUnion && !trySelectSingleNonNullTypeFromUnion()) {
            trySelectTypeFromUnion(Schema.Type.INT, Schema.Type.STRING) ||
                throw unsupportedWriterTypeError(Schema.Type.INT, Schema.Type.STRING)
        }
        when (currentWriterSchema.type) {
            Schema.Type.INT -> encodeIntUnchecked(value.code)
            Schema.Type.STRING -> encodeStringUnchecked(value.toString())
            else -> throw unsupportedWriterTypeError(Schema.Type.INT, Schema.Type.STRING)
        }
    }

    override fun encodeString(value: String) {
        if (currentWriterSchema.isUnion && !trySelectSingleNonNullTypeFromUnion()) {
            trySelectTypeFromUnion(Schema.Type.STRING, Schema.Type.BYTES) ||
                trySelectFixedSchemaForSize(value.length) ||
                trySelectEnumSchemaForSymbol(value) ||
                trySelectTypeFromUnion(
                    Schema.Type.BOOLEAN,
                    Schema.Type.INT,
                    Schema.Type.LONG,
                    Schema.Type.FLOAT,
                    Schema.Type.DOUBLE
                ) ||
                throw unsupportedWriterTypeError(
                    Schema.Type.BOOLEAN,
                    Schema.Type.INT,
                    Schema.Type.LONG,
                    Schema.Type.FLOAT,
                    Schema.Type.DOUBLE,
                    Schema.Type.STRING,
                    Schema.Type.BYTES,
                    Schema.Type.FIXED,
                    Schema.Type.ENUM
                )
        }
        when (currentWriterSchema.type) {
            Schema.Type.BOOLEAN -> encodeBooleanUnchecked(value.toBooleanStrict())
            Schema.Type.INT -> encodeIntUnchecked(value.toInt())
            Schema.Type.LONG -> encodeLongUnchecked(value.toLong())
            Schema.Type.FLOAT -> encodeFloatUnchecked(value.toFloat())
            Schema.Type.DOUBLE -> encodeDoubleUnchecked(value.toDouble())
            Schema.Type.STRING -> encodeStringUnchecked(value)
            Schema.Type.BYTES -> encodeBytesUnchecked(value.encodeToByteArray())
            Schema.Type.FIXED -> encodeFixedUnchecked(ensureFixedSize(value.encodeToByteArray()))
            Schema.Type.ENUM -> encodeEnumUnchecked(value)
            else -> throw unsupportedWriterTypeError(
                Schema.Type.BOOLEAN,
                Schema.Type.INT,
                Schema.Type.LONG,
                Schema.Type.FLOAT,
                Schema.Type.DOUBLE,
                Schema.Type.STRING,
                Schema.Type.BYTES,
                Schema.Type.FIXED,
                Schema.Type.ENUM
            )
        }
    }
}