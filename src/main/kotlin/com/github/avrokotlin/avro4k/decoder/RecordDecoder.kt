package com.github.avrokotlin.avro4k.decoder

import com.github.avrokotlin.avro4k.AvroConfiguration
import com.github.avrokotlin.avro4k.AvroEnumDefault
import com.github.avrokotlin.avro4k.schema.extractNonNull
import com.github.avrokotlin.avro4k.schema.findAnnotation
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.PolymorphicKind
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.encoding.AbstractDecoder
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema
import org.apache.avro.generic.GenericFixed
import org.apache.avro.generic.GenericRecord
import java.nio.ByteBuffer

interface ExtendedDecoder : Decoder {
    fun decodeAny(): Any?
}

interface FieldDecoder : ExtendedDecoder {
    fun fieldSchema(): Schema
}

@ExperimentalSerializationApi
class RecordDecoder(
    private val desc: SerialDescriptor,
    private val record: GenericRecord,
    override val serializersModule: SerializersModule,
    private val configuration: AvroConfiguration,
) : AbstractDecoder(), FieldDecoder {
    private var currentIndex = -1

    @Suppress("UNCHECKED_CAST")
    override fun beginStructure(descriptor: SerialDescriptor): CompositeDecoder {
        val value = fieldValueNonNull()
        return when (descriptor.kind) {
            StructureKind.CLASS -> RecordDecoder(descriptor, value as GenericRecord, serializersModule, configuration)
            StructureKind.MAP ->
                MapDecoder(
                    fieldSchema(),
                    value as Map<String, *>,
                    serializersModule,
                    configuration
                )

            StructureKind.LIST -> {
                val decoder: CompositeDecoder =
                    if (descriptor.getElementDescriptor(0).kind == PrimitiveKind.BYTE) {
                        when (value) {
                            is List<*> -> ByteArrayDecoder((value as List<Byte>).toByteArray(), serializersModule)
                            is Array<*> -> ByteArrayDecoder((value as Array<Byte>).toByteArray(), serializersModule)
                            is ByteArray -> ByteArrayDecoder(value, serializersModule)
                            is ByteBuffer -> ByteArrayDecoder(value.array(), serializersModule)
                            is GenericFixed -> ByteArrayDecoder(value.bytes(), serializersModule)
                            else -> this
                        }
                    } else {
                        when (value) {
                            is List<*> -> ListDecoder(fieldSchema(), value, serializersModule, configuration)
                            is Array<*> -> ListDecoder(fieldSchema(), value.asList(), serializersModule, configuration)
                            else -> this
                        }
                    }
                decoder
            }

            PolymorphicKind.SEALED, PolymorphicKind.OPEN ->
                UnionDecoder(
                    descriptor,
                    value as GenericRecord,
                    serializersModule,
                    configuration
                )

            else -> throw UnsupportedOperationException("Decoding descriptor of kind ${descriptor.kind} is currently not supported")
        }
    }

    private fun fieldValue(): Any? {
        if (record.hasField(resolvedFieldName())) {
            return record.get(resolvedFieldName())
        }

        return null
    }

    private fun fieldValueNonNull(): Any {
        val resolvedFieldName = resolvedFieldName()
        if (record.hasField(resolvedFieldName)) {
            return record.get(resolvedFieldName)
                ?: throw SerializationException("Field $resolvedFieldName must not be null")
        }

        throw SerializationException("Missing field $resolvedFieldName in record ${record.schema}")
    }

    private fun resolvedFieldName(): String = configuration.fieldNamingStrategy.resolve(desc, currentIndex, desc.getElementName(currentIndex))

    private fun field(): Schema.Field = record.schema.getField(resolvedFieldName())

    override fun fieldSchema(): Schema {
        // if the element is nullable, then we should have a union schema which we can extract the non-null schema from
        val schema = field().schema()
        return if (schema.isNullable) {
            schema.extractNonNull()
        } else {
            schema
        }
    }

    override fun decodeString(): String = StringFromAvroValue.fromValue(fieldValueNonNull())

    override fun decodeBoolean(): Boolean {
        return when (val v = fieldValueNonNull()) {
            is Boolean -> v
            else -> throw SerializationException("Unsupported type for Boolean ${v::class.qualifiedName}")
        }
    }

    override fun decodeAny(): Any? = fieldValue()

    override fun decodeByte(): Byte {
        return when (val v = fieldValueNonNull()) {
            is Byte -> v
            is Int -> if (v < 255) v.toByte() else throw SerializationException("Out of bound integer cannot be converted to byte [$v]")
            else -> throw SerializationException("Unsupported type for Byte ${v::class.qualifiedName}")
        }
    }

    override fun decodeNotNullMark(): Boolean {
        return fieldValue() != null
    }

    override fun decodeEnum(enumDescriptor: SerialDescriptor): Int {
        val symbol = fieldValueNonNull().toString()
        val enumIndex = enumDescriptor.getElementIndex(symbol)

        if (enumIndex != CompositeDecoder.UNKNOWN_NAME) {
            return enumIndex
        }

        return enumDescriptor.findAnnotation<AvroEnumDefault>()?.value
            ?.let { enumDescriptor.getElementIndex(it) } ?: -1
    }

    override fun decodeFloat(): Float {
        return when (val v = fieldValueNonNull()) {
            is Float -> v
            else -> throw SerializationException("Unsupported type for Float ${v::class.qualifiedName}")
        }
    }

    override fun decodeInt(): Int {
        return when (val v = fieldValueNonNull()) {
            is Int -> v
            else -> throw SerializationException("Unsupported type for Int ${v::class.qualifiedName}")
        }
    }

    override fun decodeShort(): Short {
        return when (val v = fieldValueNonNull()) {
            is Short -> v
            is Int -> v.toShort()
            else -> throw SerializationException("Unsupported type for Short ${v.javaClass}")
        }
    }

    override fun decodeLong(): Long {
        return when (val v = fieldValueNonNull()) {
            is Long -> v
            is Int -> v.toLong()
            else -> throw SerializationException("Unsupported type for Long [is ${v::class.qualifiedName}]")
        }
    }

    override fun decodeDouble(): Double {
        return when (val v = fieldValueNonNull()) {
            is Double -> v
            is Float -> v.toDouble()
            else -> throw SerializationException("Unsupported type for Double ${v::class.qualifiedName}")
        }
    }

    override fun decodeChar(): Char {
        return when (val v = fieldValueNonNull()) {
            is Int -> v.toChar()
            is Char -> v
            is CharSequence -> v.single()
            else -> throw SerializationException("Unsupported type for Char ${v::class.qualifiedName}")
        }
    }

    override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        currentIndex++
        return if (currentIndex < descriptor.elementsCount) currentIndex else CompositeDecoder.DECODE_DONE
    }
}