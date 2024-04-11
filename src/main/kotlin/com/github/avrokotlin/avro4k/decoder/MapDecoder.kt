package com.github.avrokotlin.avro4k.decoder

import com.github.avrokotlin.avro4k.AvroInternalConfiguration
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.PolymorphicKind
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.encoding.AbstractDecoder
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema
import org.apache.avro.generic.GenericArray
import org.apache.avro.generic.GenericRecord
import java.nio.ByteBuffer

@ExperimentalSerializationApi
class MapDecoder(
    private val schema: Schema,
    map: Map<*, *>,
    override val serializersModule: SerializersModule,
    private val configuration: AvroInternalConfiguration,
) : AbstractDecoder(), CompositeDecoder {
    init {
        require(schema.type == Schema.Type.MAP)
    }

    private val entries = map.toList()
    private var index = -1

    override fun decodeString(): String {
        val value = keyOrValue()
        return StringFromAvroValue.fromValue(value)
    }

    private fun keyOrValue() = if (expectKey()) key() else value()

    private fun expectKey() = index % 2 == 0

    private fun key(): Any? = entries[index / 2].first

    private fun value(): Any? = entries[index / 2].second

    override fun decodeNotNullMark(): Boolean {
        return keyOrValue() != null
    }

    override fun decodeFloat(): Float {
        return when (val v = keyOrValue()) {
            is Float -> v
            is CharSequence -> v.toString().toFloat()
            null -> throw SerializationException("Cannot decode <null> as a Float")
            else -> throw SerializationException("Unsupported type for Float ${v::class.qualifiedName}")
        }
    }

    override fun decodeInt(): Int {
        return when (val v = keyOrValue()) {
            is Int -> v
            is CharSequence -> v.toString().toInt()
            null -> throw SerializationException("Cannot decode <null> as a Int")
            else -> throw SerializationException("Unsupported type for Int ${v::class.qualifiedName}")
        }
    }

    override fun decodeLong(): Long {
        return when (val v = keyOrValue()) {
            is Long -> v
            is Int -> v.toLong()
            is CharSequence -> v.toString().toLong()
            null -> throw SerializationException("Cannot decode <null> as a Long")
            else -> throw SerializationException("Unsupported type for Long ${v::class.qualifiedName}")
        }
    }

    override fun decodeDouble(): Double {
        return when (val v = keyOrValue()) {
            is Double -> v
            is Float -> v.toDouble()
            is CharSequence -> v.toString().toDouble()
            null -> throw SerializationException("Cannot decode <null> as a Double")
            else -> throw SerializationException("Unsupported type for Double ${v::class.qualifiedName}")
        }
    }

    override fun decodeByte(): Byte {
        return when (val v = keyOrValue()) {
            is Byte -> v
            is Int -> v.toByte()
            is CharSequence -> v.toString().toByte()
            null -> throw SerializationException("Cannot decode <null> as a Byte")
            else -> throw SerializationException("Unsupported type for Byte ${v::class.qualifiedName}")
        }
    }

    override fun decodeChar(): Char {
        return when (val v = keyOrValue()) {
            is Char -> v
            is Int -> v.toChar()
            is CharSequence -> v.first()
            null -> throw SerializationException("Cannot decode <null> as a Char")
            else -> throw SerializationException("Unsupported type for Char ${v::class.qualifiedName}")
        }
    }

    override fun decodeShort(): Short {
        return when (val v = keyOrValue()) {
            is Short -> v
            is Int -> v.toShort()
            is CharSequence -> v.toString().toShort()
            null -> throw SerializationException("Cannot decode <null> as a Byte")
            else -> throw SerializationException("Unsupported type for Byte ${v::class.qualifiedName}")
        }
    }

    override fun decodeEnum(enumDescriptor: SerialDescriptor): Int {
        return when (val v = keyOrValue()) {
            is CharSequence -> enumDescriptor.getElementIndex(v.toString())
            null -> throw SerializationException("Cannot decode <null> as a $enumDescriptor")
            else -> throw SerializationException("Unsupported type for $enumDescriptor: ${v::class.qualifiedName}")
        }
    }

    override fun decodeBoolean(): Boolean {
        return when (val v = keyOrValue()) {
            is Boolean -> v
            is CharSequence -> v.toString().toBooleanStrict()
            null -> throw SerializationException("Cannot decode <null> as a Boolean")
            else -> throw SerializationException("Unsupported type for Boolean. Actual: ${v::class.qualifiedName}")
        }
    }

    override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        index++
        return if (index == entries.size * 2) CompositeDecoder.DECODE_DONE else index
    }

    @Suppress("UNCHECKED_CAST")
    override fun beginStructure(descriptor: SerialDescriptor): CompositeDecoder {
        return when (descriptor.kind) {
            StructureKind.CLASS -> RecordDecoder(descriptor, value() as GenericRecord, serializersModule, configuration)
            StructureKind.LIST ->
                when (descriptor.getElementDescriptor(0).kind) {
                    PrimitiveKind.BYTE -> ByteArrayDecoder((value() as ByteBuffer).array(), serializersModule)
                    else -> ListDecoder(schema.valueType, value() as GenericArray<*>, serializersModule, configuration)
                }
            StructureKind.MAP -> MapDecoder(schema.valueType, value() as Map<String, *>, serializersModule, configuration)
            PolymorphicKind.SEALED, PolymorphicKind.OPEN -> UnionDecoder(descriptor, value() as GenericRecord, serializersModule, configuration)
            else -> throw UnsupportedOperationException("Kind ${descriptor.kind} is currently not supported.")
        }
    }
}