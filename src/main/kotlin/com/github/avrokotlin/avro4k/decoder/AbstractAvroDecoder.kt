package com.github.avrokotlin.avro4k.decoder

import com.github.avrokotlin.avro4k.AnnotationExtractor
import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.getSchemaNameForUnion
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.PolymorphicKind
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericEnumSymbol
import org.apache.avro.generic.GenericRecord
import java.nio.ByteBuffer

@ExperimentalSerializationApi
abstract class AbstractAvroDecoder : Decoder, ExtendedDecoder {
    abstract val avro: Avro
    final override val serializersModule: SerializersModule
        get() = avro.serializersModule

    @Suppress("UNCHECKED_CAST")
    final override fun beginStructure(descriptor: SerialDescriptor): CompositeDecoder {
        val value = decodeAny()
        return when (descriptor.kind) {
            StructureKind.CLASS, StructureKind.OBJECT -> RecordDecoder(value as GenericRecord, avro)
            StructureKind.MAP -> MapDecoder(currentSchema, value as Map<String, *>, avro)
            StructureKind.LIST -> when (descriptor.getElementDescriptor(0).kind) {
                PrimitiveKind.BYTE -> when (value) {
                    is List<*> -> ByteArrayDecoder((value as List<Byte>).toByteArray(), serializersModule)
                    is Array<*> -> ByteArrayDecoder((value as Array<Byte>).toByteArray(), serializersModule)
                    is ByteArray -> ByteArrayDecoder(value, serializersModule)
                    is ByteBuffer -> ByteArrayDecoder(value.array(), serializersModule)
                    else -> throw SerializationException("Unable to decode ${value::class} as byte array")
                }

                else -> when (value) {
                    is List<*> -> ListDecoder(currentSchema, value, avro)
                    is Array<*> -> ListDecoder(currentSchema, value.asList(), avro)
                    else -> throw SerializationException("Unable to decode ${value::class} as byte array")
                }
            }

            is PolymorphicKind -> AvroPolymorphicDecoder(value as GenericRecord)

            else -> throw UnsupportedOperationException("Decoding descriptor of kind ${descriptor.kind} is currently not supported")
        }
    }

    override fun decodeString() = when (val value = decodeAny()) {
        is CharSequence -> value.toString()
        is GenericData.Fixed -> String(value.bytes())
        is ByteArray -> String(value)
        is ByteBuffer -> String(value.array())
        else -> throw SerializationException("Unsupported type for String [is ${value.javaClass}]")
    }

    final override fun decodeBoolean() = when (val value = decodeAny()) {
        is Boolean -> value
        "true" -> true
        "false" -> false
        else -> throw SerializationException("Unsupported type for Boolean ${value::class}")
    }

    final override fun decodeByte() = when (val value = decodeAny()) {
        is Byte -> value
        is Int -> if (value <= Byte.MAX_VALUE && value >= Byte.MIN_VALUE) value.toByte() else throw SerializationException(
            "Out of bound integer cannot be converted to byte [$value]"
        )

        else -> throw SerializationException("Unsupported type for Byte ${value::class}")
    }

    @ExperimentalSerializationApi
    final override fun decodeEnum(enumDescriptor: SerialDescriptor): Int {
        val symbol = when (val value = decodeAny()) {
            is GenericEnumSymbol<*> -> value.toString()
            is String -> value
            else -> value.toString()
        }
        return enumDescriptor.getElementIndex(symbol).takeIf { it >= 0 }
            ?: AnnotationExtractor(enumDescriptor.annotations).enumDefault()?.let {
                enumDescriptor.getElementIndex(it).takeIf { it >= 0 }
            }
            ?: CompositeDecoder.UNKNOWN_NAME
    }

    final override fun decodeFloat() = when (val value = decodeAny()) {
        is Float -> value
        else -> throw SerializationException("Unsupported type for Float ${value::class}")
    }

    final override fun decodeInt() = when (val value = decodeAny()) {
        is Int -> value
        else -> throw SerializationException("Unsupported type for Int ${value::class}")
    }

    final override fun decodeLong() = when (val value = decodeAny()) {
        is Long -> value
        is Int -> value.toLong()
        else -> throw SerializationException("Unsupported type for Long [is ${value::class}]")
    }

    final override fun decodeDouble() = when (val value = decodeAny()) {
        is Double -> value
        is Float -> value.toDouble()
        else -> throw SerializationException("Unsupported type for Double ${value::class}")
    }

    final override fun decodeInline(descriptor: SerialDescriptor) = this

    final override fun decodeChar(): Char {
        val value = decodeAny()
        return when {
            value is Char -> value
            value is String && value.length == 1 -> value[0]
            value is Int -> Char(value)
            else -> throw SerializationException("Unsupported type for Char ${value::class}")
        }
    }

    final override fun decodeShort() = when (val value = decodeAny()) {
        is Short -> value
        is Int -> value.toShort()
        else -> throw SerializationException("Unsupported type for Short ${value::class}")
    }

    final override fun decodeNull() = null

    @ExperimentalSerializationApi
    inner class AvroPolymorphicDecoder(private val value: GenericRecord) : PolymorphicDecoder() {
        override fun decodeSerialName(
            polymorphicDescriptor: SerialDescriptor,
            possibleSubclasses: Sequence<SerialDescriptor>
        ): String {
            return possibleSubclasses.firstOrNull { it.getSchemaNameForUnion(avro.nameResolver).fullName == value.schema.fullName }
                ?.serialName
                ?: throw SerializationException("Cannot find a subtype of ${polymorphicDescriptor.serialName} that can be used to deserialize a record of schema ${value.schema}.")
        }

        override fun <T> decodeValue(deserializer: DeserializationStrategy<T>): T {
            return this@AbstractAvroDecoder.decodeSerializableValue(deserializer)
        }

        override val serializersModule: SerializersModule
            get() = this@AbstractAvroDecoder.serializersModule
    }
}