package com.github.avrokotlin.avro4k.internal.decoder.generic

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroDecoder
import com.github.avrokotlin.avro4k.internal.BadDecodedValueError
import com.github.avrokotlin.avro4k.internal.SerializerLocatorMiddleware
import com.github.avrokotlin.avro4k.internal.toByteExact
import com.github.avrokotlin.avro4k.internal.toDoubleExact
import com.github.avrokotlin.avro4k.internal.toFloatExact
import com.github.avrokotlin.avro4k.internal.toIntExact
import com.github.avrokotlin.avro4k.internal.toLongExact
import com.github.avrokotlin.avro4k.internal.toShortExact
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.PolymorphicKind
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.SerialKind
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.encoding.AbstractDecoder
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.generic.GenericArray
import org.apache.avro.generic.GenericContainer
import org.apache.avro.generic.GenericEnumSymbol
import org.apache.avro.generic.GenericFixed
import org.apache.avro.generic.IndexedRecord
import java.math.BigDecimal
import java.nio.ByteBuffer

internal abstract class AbstractAvroGenericDecoder : AbstractDecoder(), AvroDecoder {
    internal abstract val avro: Avro

    abstract override fun decodeNotNullMark(): Boolean

    abstract override fun decodeValue(): Any

    override val serializersModule: SerializersModule
        get() = avro.serializersModule

    override fun <T> decodeSerializableValue(deserializer: DeserializationStrategy<T>): T {
        return SerializerLocatorMiddleware.apply(deserializer)
            .deserialize(this)
    }

    @Suppress("UNCHECKED_CAST")
    override fun beginStructure(descriptor: SerialDescriptor): CompositeDecoder {
        return when (descriptor.kind) {
            StructureKind.LIST ->
                when (val value = decodeValue()) {
                    is GenericArray<*> ->
                        ArrayGenericDecoder(
                            collection = value,
                            writerSchema = value.schema,
                            avro = avro
                        )

                    is Collection<*> ->
                        ArrayGenericDecoder(
                            collection = value,
                            writerSchema = currentWriterSchema,
                            avro = avro
                        )

                    else -> throw BadDecodedValueError(value, StructureKind.LIST, GenericArray::class, Collection::class, ByteBuffer::class)
                }

            StructureKind.MAP ->
                when (val value = decodeValue()) {
                    is Map<*, *> ->
                        MapGenericDecoder(
                            value as Map<CharSequence, Any?>,
                            currentWriterSchema,
                            avro
                        )

                    else -> throw BadDecodedValueError(value, StructureKind.MAP, Map::class)
                }

            StructureKind.CLASS, StructureKind.OBJECT ->
                when (val value = decodeValue()) {
                    is IndexedRecord -> RecordGenericDecoder(value, descriptor, avro)
                    else -> throw BadDecodedValueError(value, descriptor.kind, IndexedRecord::class)
                }

            is PolymorphicKind ->
                when (val value = decodeValue()) {
                    is GenericContainer -> PolymorphicGenericDecoder(avro, descriptor, value.schema, value)
                    else -> PolymorphicGenericDecoder(avro, descriptor, currentWriterSchema, value)
                }

            else -> throw SerializationException("Unsupported descriptor for structure decoding: $descriptor")
        }
    }

    override fun decodeBoolean(): Boolean {
        return when (val value = decodeValue()) {
            is Boolean -> value
            is CharSequence -> value.toString().toBoolean()
            else -> throw BadDecodedValueError(value, PrimitiveKind.BOOLEAN, Boolean::class, Int::class, CharSequence::class)
        }
    }

    override fun decodeByte(): Byte {
        return when (val value = decodeValue()) {
            is Int -> value.toByteExact()
            is Long -> value.toByteExact()
            is BigDecimal -> value.toByteExact()
            is CharSequence -> value.toString().toByte()
            else -> throw BadDecodedValueError(value, PrimitiveKind.BYTE, Int::class, Long::class, BigDecimal::class, CharSequence::class)
        }
    }

    override fun decodeShort(): Short {
        return when (val value = decodeValue()) {
            is Int -> value.toShortExact()
            is Long -> value.toShortExact()
            is BigDecimal -> value.toShortExact()
            is CharSequence -> value.toString().toShort()
            else -> throw BadDecodedValueError(value, PrimitiveKind.SHORT, Int::class, Long::class, BigDecimal::class, CharSequence::class)
        }
    }

    override fun decodeInt(): Int {
        return when (val value = decodeValue()) {
            is Int -> value
            is Long -> value.toIntExact()
            is BigDecimal -> value.toIntExact()
            is CharSequence -> value.toString().toInt()
            else -> throw BadDecodedValueError(value, PrimitiveKind.INT, Int::class, Long::class, BigDecimal::class, CharSequence::class)
        }
    }

    override fun decodeLong(): Long {
        return when (val value = decodeValue()) {
            is Long -> value
            is Int -> value.toLong()
            is BigDecimal -> value.toLongExact()
            is CharSequence -> value.toString().toLong()
            else -> throw BadDecodedValueError(value, PrimitiveKind.LONG, Int::class, Long::class, BigDecimal::class, CharSequence::class)
        }
    }

    override fun decodeFloat(): Float {
        return when (val value = decodeValue()) {
            is Float -> value
            is Double -> value.toFloatExact()
            is BigDecimal -> value.toFloatExact()
            is CharSequence -> value.toString().toFloat()
            else -> throw BadDecodedValueError(value, PrimitiveKind.FLOAT, Float::class, Double::class, BigDecimal::class, CharSequence::class)
        }
    }

    override fun decodeDouble(): Double {
        return when (val value = decodeValue()) {
            is Double -> value
            is Float -> value.toDouble()
            is BigDecimal -> value.toDoubleExact()
            is CharSequence -> value.toString().toDouble()
            else -> throw BadDecodedValueError(value, PrimitiveKind.DOUBLE, Float::class, Double::class, BigDecimal::class, CharSequence::class)
        }
    }

    override fun decodeChar(): Char {
        return when (val value = decodeValue()) {
            is Int -> value.toChar()
            is CharSequence -> value.single()
            else -> throw BadDecodedValueError(value, PrimitiveKind.CHAR, Int::class, CharSequence::class)
        }
    }

    override fun decodeString(): String {
        return when (val value = decodeValue()) {
            is CharSequence -> value.toString()
            is ByteArray -> value.decodeToString()
            is GenericFixed -> value.bytes().decodeToString()
            else -> throw BadDecodedValueError(value, PrimitiveKind.STRING, CharSequence::class, ByteArray::class, GenericFixed::class)
        }
    }

    override fun decodeEnum(enumDescriptor: SerialDescriptor): Int {
        return when (val value = decodeValue()) {
            is GenericEnumSymbol<*>, is CharSequence -> {
                enumDescriptor.getElementIndex(value.toString()).takeIf { it >= 0 }
                    ?: avro.enumResolver.getDefaultValueIndex(enumDescriptor)
                    ?: throw SerializationException("Unknown enum symbol '$value' for Enum '${enumDescriptor.serialName}'")
            }

            else -> throw BadDecodedValueError(value, SerialKind.ENUM, GenericEnumSymbol::class, CharSequence::class)
        }
    }

    override fun decodeBytes(): ByteArray {
        return when (val value = decodeValue()) {
            is ByteArray -> value
            is ByteBuffer -> value.array()
            is GenericFixed -> value.bytes()
            is CharSequence -> value.toString().toByteArray()
            else -> throw BadDecodedValueError<ByteArray>(value, ByteArray::class, GenericFixed::class, CharSequence::class)
        }
    }

    override fun decodeFixed(): GenericFixed {
        return when (val value = decodeValue()) {
            is GenericFixed -> value
            else -> throw BadDecodedValueError<GenericFixed>(value, GenericFixed::class)
        }
    }
}