package com.github.avrokotlin.avro4k.internal.decoder.direct

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.UnionDecoder
import com.github.avrokotlin.avro4k.internal.SerializerLocatorMiddleware
import com.github.avrokotlin.avro4k.internal.decoder.AbstractPolymorphicDecoder
import com.github.avrokotlin.avro4k.internal.isFullNameOrAliasMatch
import com.github.avrokotlin.avro4k.internal.toByteExact
import com.github.avrokotlin.avro4k.internal.toFloatExact
import com.github.avrokotlin.avro4k.internal.toIntExact
import com.github.avrokotlin.avro4k.internal.toShortExact
import com.github.avrokotlin.avro4k.unsupportedWriterTypeError
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.PolymorphicKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericFixed

internal abstract class AbstractAvroDirectDecoder(
    protected val avro: Avro,
    protected val binaryDecoder: org.apache.avro.io.Decoder,
) : AbstractInterceptingDecoder(), UnionDecoder {
    abstract override var currentWriterSchema: Schema
    internal var decodedCollectionSize = -1

    override val serializersModule: SerializersModule
        get() = avro.serializersModule

    @Deprecated("Do not use it for direct encoding")
    final override fun decodeValue(): Any {
        throw UnsupportedOperationException("Direct decoding doesn't support decoding generic values")
    }

    override fun <T> decodeSerializableValue(deserializer: DeserializationStrategy<T>): T {
        return SerializerLocatorMiddleware.apply(deserializer)
            .deserialize(this)
    }

    override fun beginStructure(descriptor: SerialDescriptor): CompositeDecoder {
        decodeAndResolveUnion()

        return when (descriptor.kind) {
            StructureKind.LIST ->
                when (currentWriterSchema.type) {
                    Schema.Type.ARRAY ->
                        ArrayBlockDirectDecoder(
                            currentWriterSchema,
                            decodeFirstBlock = decodedCollectionSize == -1,
                            { decodedCollectionSize = it },
                            avro,
                            binaryDecoder
                        )
                    else -> throw unsupportedWriterTypeError(Schema.Type.ARRAY)
                }

            StructureKind.MAP ->
                when (currentWriterSchema.type) {
                    Schema.Type.MAP ->
                        MapBlockDirectDecoder(
                            currentWriterSchema,
                            decodeFirstBlock = decodedCollectionSize == -1,
                            { decodedCollectionSize = it },
                            avro,
                            binaryDecoder
                        )
                    else -> throw unsupportedWriterTypeError(Schema.Type.MAP)
                }

            StructureKind.CLASS, StructureKind.OBJECT ->
                when (currentWriterSchema.type) {
                    Schema.Type.RECORD -> RecordDirectDecoder(currentWriterSchema, descriptor, avro, binaryDecoder)
                    else -> throw unsupportedWriterTypeError(Schema.Type.RECORD)
                }

            is PolymorphicKind -> PolymorphicDecoder(avro, descriptor, currentWriterSchema, binaryDecoder)
            else -> throw SerializationException("Unsupported descriptor for structure decoding: $descriptor")
        }
    }

    override fun decodeAndResolveUnion() {
        if (currentWriterSchema.isUnion) {
            currentWriterSchema = currentWriterSchema.types[binaryDecoder.readIndex()]
        }
    }

    override fun decodeNotNullMark(): Boolean {
        decodeAndResolveUnion()

        return currentWriterSchema.type != Schema.Type.NULL
    }

    override fun decodeNull(): Nothing? {
        decodeAndResolveUnion()

        if (currentWriterSchema.type != Schema.Type.NULL) {
            throw unsupportedWriterTypeError(Schema.Type.NULL)
        }
        binaryDecoder.readNull()
        return null
    }

    override fun decodeBoolean(): Boolean {
        decodeAndResolveUnion()

        return when (currentWriterSchema.type) {
            Schema.Type.BOOLEAN -> binaryDecoder.readBoolean()
            Schema.Type.STRING -> binaryDecoder.readString().toBooleanStrict()
            else -> throw unsupportedWriterTypeError(Schema.Type.BOOLEAN, Schema.Type.STRING)
        }
    }

    override fun decodeByte(): Byte {
        return decodeInt().toByteExact()
    }

    override fun decodeShort(): Short {
        return decodeInt().toShortExact()
    }

    override fun decodeInt(): Int {
        decodeAndResolveUnion()

        return when (currentWriterSchema.type) {
            Schema.Type.INT -> binaryDecoder.readInt()
            Schema.Type.LONG -> binaryDecoder.readLong().toIntExact()
            Schema.Type.STRING -> binaryDecoder.readString().toInt()
            else -> throw unsupportedWriterTypeError(Schema.Type.INT, Schema.Type.LONG, Schema.Type.STRING)
        }
    }

    override fun decodeLong(): Long {
        decodeAndResolveUnion()

        return when (currentWriterSchema.type) {
            Schema.Type.LONG -> binaryDecoder.readLong()
            Schema.Type.INT -> binaryDecoder.readInt().toLong()
            Schema.Type.STRING -> binaryDecoder.readString().toLong()
            else -> throw unsupportedWriterTypeError(Schema.Type.LONG, Schema.Type.INT, Schema.Type.STRING)
        }
    }

    override fun decodeFloat(): Float {
        decodeAndResolveUnion()

        return when (currentWriterSchema.type) {
            Schema.Type.FLOAT -> binaryDecoder.readFloat()
            Schema.Type.DOUBLE -> binaryDecoder.readDouble().toFloatExact()
            Schema.Type.STRING -> binaryDecoder.readString().toFloat()
            else -> throw unsupportedWriterTypeError(Schema.Type.FLOAT, Schema.Type.DOUBLE, Schema.Type.STRING)
        }
    }

    override fun decodeDouble(): Double {
        decodeAndResolveUnion()

        return when (currentWriterSchema.type) {
            Schema.Type.FLOAT -> binaryDecoder.readFloat().toDouble()
            Schema.Type.DOUBLE -> binaryDecoder.readDouble()
            Schema.Type.STRING -> binaryDecoder.readString().toDouble()
            else -> throw unsupportedWriterTypeError(Schema.Type.DOUBLE, Schema.Type.FLOAT, Schema.Type.STRING)
        }
    }

    override fun decodeChar(): Char {
        decodeAndResolveUnion()

        return when (currentWriterSchema.type) {
            Schema.Type.INT -> binaryDecoder.readInt().toChar()
            Schema.Type.STRING -> binaryDecoder.readString(null).single()
            else -> throw unsupportedWriterTypeError(Schema.Type.INT, Schema.Type.STRING)
        }
    }

    override fun decodeString(): String {
        decodeAndResolveUnion()

        return when (currentWriterSchema.type) {
            Schema.Type.STRING -> binaryDecoder.readString()
            Schema.Type.BYTES -> binaryDecoder.readBytes().decodeToString()
            Schema.Type.FIXED -> binaryDecoder.readFixedBytes(currentWriterSchema.fixedSize).decodeToString()
            Schema.Type.BOOLEAN -> binaryDecoder.readBoolean().toString()
            Schema.Type.INT -> binaryDecoder.readInt().toString()
            Schema.Type.LONG -> binaryDecoder.readLong().toString()
            Schema.Type.FLOAT -> binaryDecoder.readFloat().toString()
            Schema.Type.DOUBLE -> binaryDecoder.readDouble().toString()
            Schema.Type.ENUM -> currentWriterSchema.enumSymbols[binaryDecoder.readEnum()]
            else -> throw unsupportedWriterTypeError(
                Schema.Type.STRING,
                Schema.Type.BYTES,
                Schema.Type.FIXED,
                Schema.Type.BOOLEAN,
                Schema.Type.INT,
                Schema.Type.LONG,
                Schema.Type.FLOAT,
                Schema.Type.DOUBLE,
                Schema.Type.ENUM
            )
        }
    }

    override fun decodeEnum(enumDescriptor: SerialDescriptor): Int {
        decodeAndResolveUnion()

        return when (currentWriterSchema.type) {
            Schema.Type.ENUM -> {
                if (currentWriterSchema.isFullNameOrAliasMatch(enumDescriptor)) {
                    val enumSymbol = currentWriterSchema.enumSymbols[binaryDecoder.readEnum()]
                    enumDescriptor.getEnumIndex(enumSymbol)
                } else {
                    throw unsupportedWriterTypeError(Schema.Type.ENUM, Schema.Type.STRING)
                }
            }

            Schema.Type.STRING -> {
                val enumSymbol = binaryDecoder.readString()
                enumDescriptor.getEnumIndex(enumSymbol)
            }

            else -> throw unsupportedWriterTypeError(Schema.Type.ENUM, Schema.Type.STRING)
        }
    }

    private fun SerialDescriptor.getEnumIndex(enumName: String): Int {
        val idx = getElementIndex(enumName)
        return if (idx >= 0) {
            idx
        } else {
            avro.enumResolver.getDefaultValueIndex(this)
                ?: throw SerializationException("Unknown enum symbol name '$enumName' for Enum '${this.serialName}' for writer schema $currentWriterSchema")
        }
    }

    override fun decodeBytes(): ByteArray {
        decodeAndResolveUnion()

        return when (currentWriterSchema.type) {
            Schema.Type.BYTES -> binaryDecoder.readBytes()
            Schema.Type.FIXED -> binaryDecoder.readFixedBytes(currentWriterSchema.fixedSize)
            Schema.Type.STRING -> binaryDecoder.readString(null).bytes
            else -> throw unsupportedWriterTypeError(Schema.Type.BYTES, Schema.Type.FIXED, Schema.Type.STRING)
        }
    }

    override fun decodeFixed(): GenericFixed {
        decodeAndResolveUnion()

        return when (currentWriterSchema.type) {
            Schema.Type.BYTES -> GenericData.Fixed(currentWriterSchema, binaryDecoder.readBytes())
            Schema.Type.FIXED -> GenericData.Fixed(currentWriterSchema, binaryDecoder.readFixedBytes(currentWriterSchema.fixedSize))
            Schema.Type.STRING -> GenericData.Fixed(currentWriterSchema, binaryDecoder.readString(null).bytes)
            else -> throw unsupportedWriterTypeError(Schema.Type.FIXED, Schema.Type.BYTES, Schema.Type.STRING)
        }
    }
}

private fun org.apache.avro.io.Decoder.readFixedBytes(size: Int): ByteArray {
    return ByteArray(size).also { buf -> readFixed(buf) }
}

private fun org.apache.avro.io.Decoder.readBytes(): ByteArray {
    return readBytes(null).array()
}

private class PolymorphicDecoder(
    avro: Avro,
    descriptor: SerialDescriptor,
    schema: Schema,
    private val binaryDecoder: org.apache.avro.io.Decoder,
) : AbstractPolymorphicDecoder(avro, descriptor, schema) {
    override fun tryFindSerialNameForUnion(
        namesAndAliasesToSerialName: Map<String, String>,
        schema: Schema,
    ): Pair<String, Schema>? {
        return tryFindSerialName(namesAndAliasesToSerialName, schema.types[binaryDecoder.readIndex()])
    }

    override fun newDecoder(chosenSchema: Schema): Decoder {
        return AvroValueDirectDecoder(chosenSchema, avro, binaryDecoder)
    }
}