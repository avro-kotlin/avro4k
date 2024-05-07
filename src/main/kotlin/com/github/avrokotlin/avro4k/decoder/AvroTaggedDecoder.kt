package com.github.avrokotlin.avro4k.decoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.internal.BadDecodedValueError
import com.github.avrokotlin.avro4k.internal.toByteExact
import com.github.avrokotlin.avro4k.internal.toDoubleExact
import com.github.avrokotlin.avro4k.internal.toFloatExact
import com.github.avrokotlin.avro4k.internal.toIntExact
import com.github.avrokotlin.avro4k.internal.toLongExact
import com.github.avrokotlin.avro4k.internal.toShortExact
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.builtins.ByteArraySerializer
import kotlinx.serialization.descriptors.PolymorphicKind
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.SerialKind
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.internal.TaggedDecoder
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema
import org.apache.avro.generic.GenericArray
import org.apache.avro.generic.GenericEnumSymbol
import org.apache.avro.generic.GenericFixed
import org.apache.avro.generic.IndexedRecord
import java.math.BigDecimal
import java.nio.ByteBuffer

@OptIn(InternalSerializationApi::class)
internal abstract class AvroTaggedDecoder<Tag> : TaggedDecoder<Tag>(), AvroDecoder {
    internal abstract val avro: Avro

    protected abstract val Tag.writerSchema: Schema

    abstract override fun decodeTaggedNotNullMark(tag: Tag): Boolean

    abstract override fun decodeTaggedValue(tag: Tag): Any

    override val currentWriterSchema: Schema
        get() = currentTag.writerSchema

    override val serializersModule: SerializersModule
        get() = avro.serializersModule

    override fun <T> decodeSerializableValue(deserializer: DeserializationStrategy<T>): T {
        if (deserializer.descriptor == ByteArraySerializer().descriptor) {
            // fast-path for ByteArray fields, to avoid slow-path with ArrayDecoder
            @Suppress("UNCHECKED_CAST")
            return decodeBytes() as T
        }
        return super<TaggedDecoder>.decodeSerializableValue(deserializer)
    }

    @Suppress("UNCHECKED_CAST")
    override fun beginStructure(descriptor: SerialDescriptor): CompositeDecoder {
        return when (descriptor.kind) {
            StructureKind.LIST ->
                when (val value = decodeValue()) {
                    is GenericArray<*> ->
                        ArrayDecoder(
                            collection = value,
                            writerSchema = value.schema,
                            avro = avro
                        )

                    is Collection<*> ->
                        ArrayDecoder(
                            collection = value,
                            writerSchema = currentTag.writerSchema,
                            avro = avro
                        )

                    // TODO should be removed as byte arrays are handled by fast-path in decodeSerializableValue
                    //  and collection of bytes should be handled as normal arrays of byte and not as native bytes
                    is ByteBuffer -> ByteArrayDecoder(avro, value.array())

                    else -> throw BadDecodedValueError(value, StructureKind.LIST, GenericArray::class, Collection::class, ByteBuffer::class)
                }

            StructureKind.MAP ->
                when (val value = decodeValue()) {
                    is Map<*, *> ->
                        MapDecoder(
                            value as Map<CharSequence, Any?>,
                            currentTag.writerSchema,
                            avro
                        )

                    else -> throw BadDecodedValueError(value, StructureKind.MAP, Map::class)
                }

            StructureKind.CLASS, StructureKind.OBJECT ->
                when (val value = decodeValue()) {
                    is IndexedRecord -> RecordDecoder(value, descriptor, avro)
                    else -> throw BadDecodedValueError(value, descriptor.kind, IndexedRecord::class)
                }

            is PolymorphicKind ->
                when (val value = decodeValue()) {
                    is IndexedRecord -> PolymorphicDecoder(avro, descriptor, value)
                    else -> throw BadDecodedValueError(value, descriptor.kind, IndexedRecord::class)
                }

            else -> throw SerializationException("Unsupported descriptor for structure decoding: $descriptor")
        }
    }

    override fun decodeValue() = decodeTaggedValue(currentTag)

    override fun decodeTaggedBoolean(tag: Tag): Boolean {
        return when (val value = decodeTaggedValue(tag)) {
            is Boolean -> value
            1 -> true
            0 -> false
            is CharSequence -> value.toString().toBoolean()
            else -> throw BadDecodedValueError(value, PrimitiveKind.BOOLEAN, Boolean::class, Int::class, CharSequence::class)
        }
    }

    override fun decodeTaggedByte(tag: Tag): Byte {
        return when (val value = decodeTaggedValue(tag)) {
            is Int -> value.toByteExact()
            is Long -> value.toByteExact()
            is BigDecimal -> value.toByteExact()
            is CharSequence -> value.toString().toByte()
            else -> throw BadDecodedValueError(value, PrimitiveKind.BYTE, Int::class, Long::class, BigDecimal::class, CharSequence::class)
        }
    }

    override fun decodeTaggedShort(tag: Tag): Short {
        return when (val value = decodeTaggedValue(tag)) {
            is Int -> value.toShortExact()
            is Long -> value.toShortExact()
            is BigDecimal -> value.toShortExact()
            is CharSequence -> value.toString().toShort()
            else -> throw BadDecodedValueError(value, PrimitiveKind.SHORT, Int::class, Long::class, BigDecimal::class, CharSequence::class)
        }
    }

    override fun decodeTaggedInt(tag: Tag): Int {
        return when (val value = decodeTaggedValue(tag)) {
            is Int -> value
            is Long -> value.toIntExact()
            is BigDecimal -> value.toIntExact()
            is CharSequence -> value.toString().toInt()
            else -> throw BadDecodedValueError(value, PrimitiveKind.INT, Int::class, Long::class, BigDecimal::class, CharSequence::class)
        }
    }

    override fun decodeTaggedLong(tag: Tag): Long {
        return when (val value = decodeTaggedValue(tag)) {
            is Long -> value
            is Int -> value.toLong()
            is BigDecimal -> value.toLongExact()
            is CharSequence -> value.toString().toLong()
            else -> throw BadDecodedValueError(value, PrimitiveKind.LONG, Int::class, Long::class, BigDecimal::class, CharSequence::class)
        }
    }

    override fun decodeTaggedFloat(tag: Tag): Float {
        return when (val value = decodeTaggedValue(tag)) {
            is Float -> value
            is Double -> value.toFloatExact()
            is BigDecimal -> value.toFloatExact()
            is CharSequence -> value.toString().toFloat()
            else -> throw BadDecodedValueError(value, PrimitiveKind.FLOAT, Float::class, Double::class, BigDecimal::class, CharSequence::class)
        }
    }

    override fun decodeTaggedDouble(tag: Tag): Double {
        return when (val value = decodeTaggedValue(tag)) {
            is Double -> value
            is Float -> value.toDouble()
            is BigDecimal -> value.toDoubleExact()
            is CharSequence -> value.toString().toDouble()
            else -> throw BadDecodedValueError(value, PrimitiveKind.DOUBLE, Float::class, Double::class, BigDecimal::class, CharSequence::class)
        }
    }

    override fun decodeTaggedChar(tag: Tag): Char {
        val value = decodeTaggedValue(tag)
        return when {
            value is Int -> value.toChar()
            value is CharSequence && value.length == 1 -> value[0]
            else -> throw BadDecodedValueError(value, PrimitiveKind.CHAR, Int::class, CharSequence::class)
        }
    }

    override fun decodeTaggedString(tag: Tag): String {
        return when (val value = decodeTaggedValue(tag)) {
            is CharSequence -> value.toString()
            is ByteArray -> value.decodeToString()
            is GenericFixed -> value.bytes().decodeToString()
            else -> throw BadDecodedValueError(value, PrimitiveKind.STRING, CharSequence::class, ByteArray::class, GenericFixed::class)
        }
    }

    override fun decodeTaggedEnum(
        tag: Tag,
        enumDescriptor: SerialDescriptor,
    ): Int {
        return when (val value = decodeTaggedValue(tag)) {
            is GenericEnumSymbol<*>, is CharSequence -> {
                enumDescriptor.getElementIndex(value.toString()).takeIf { it >= 0 }
                    ?: avro.enumResolver.getDefaultValueIndex(enumDescriptor)
                    ?: throw SerializationException("Unknown enum symbol '$value' for Enum '${enumDescriptor.serialName}'")
            }

            else -> throw BadDecodedValueError(value, SerialKind.ENUM, GenericEnumSymbol::class, CharSequence::class)
        }
    }

    override fun decodeBytes(): ByteArray {
        return when (val value = decodeTaggedValue(currentTag)) {
            is ByteArray -> value
            is ByteBuffer -> value.array()
            is GenericFixed -> value.bytes()
            is CharSequence -> value.toString().toByteArray()
            else -> throw BadDecodedValueError<ByteArray>(value, ByteArray::class, GenericFixed::class, CharSequence::class)
        }
    }

    override fun decodeFixed(): GenericFixed {
        return when (val value = decodeTaggedValue(currentTag)) {
            is GenericFixed -> value
            else -> throw BadDecodedValueError<GenericFixed>(value, GenericFixed::class)
        }
    }
}