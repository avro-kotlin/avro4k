package com.github.avrokotlin.avro4k.decoder

import com.github.avrokotlin.avro4k.AnnotationExtractor
import com.github.avrokotlin.avro4k.AvroConfiguration
import com.github.avrokotlin.avro4k.FieldNaming
import com.github.avrokotlin.avro4k.schema.extractNonNull
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
        val valueType = AnnotationExtractor(descriptor.annotations).valueType()
        val value = fieldValue()
        return when (descriptor.kind) {
            StructureKind.CLASS ->
                if (valueType)
                    InlineDecoder(fieldValue(), serializersModule)
                else
                    RecordDecoder(descriptor, value as GenericRecord, serializersModule, configuration)
            StructureKind.MAP -> MapDecoder(
                descriptor,
                fieldSchema(),
                value as Map<String, *>,
                serializersModule,
                configuration
            )
            StructureKind.LIST -> {
                val decoder: CompositeDecoder = if (descriptor.getElementDescriptor(0).kind == PrimitiveKind.BYTE) {
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
            PolymorphicKind.SEALED, PolymorphicKind.OPEN -> UnionDecoder(
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

        FieldNaming(desc, currentIndex).aliases().forEach {
           if (record.hasField(it)) {
              return record.get(it)
           }
        }

        return null
    }

    private fun resolvedFieldName(): String = configuration.namingStrategy.to(FieldNaming(desc, currentIndex).name())

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

    override fun decodeString(): String = StringFromAvroValue.fromValue(fieldValue())

    override fun decodeBoolean(): Boolean {
        return when (val v = fieldValue()) {
            is Boolean -> v
            null -> throw SerializationException("Cannot decode <null> as a Boolean")
            else -> throw SerializationException("Unsupported type for Boolean ${v.javaClass}")
        }
    }

    override fun decodeAny(): Any? = fieldValue()

    override fun decodeByte(): Byte {
        return when (val v = fieldValue()) {
            is Byte -> v
            is Int -> if (v < 255) v.toByte() else throw SerializationException("Out of bound integer cannot be converted to byte [$v]")
            null -> throw SerializationException("Cannot decode <null> as a Byte")
            else -> throw SerializationException("Unsupported type for Byte ${v.javaClass}")
        }
    }

    override fun decodeNotNullMark(): Boolean {
        return fieldValue() != null
    }

    override fun decodeEnum(enumDescriptor: SerialDescriptor): Int {
        val symbol = EnumFromAvroValue.fromValue(fieldValue()!!)        
        val enumValueByEnumName =
            (0 until enumDescriptor.elementsCount).associateBy { enumDescriptor.getElementName(it) }

        return enumValueByEnumName[symbol] ?: AnnotationExtractor(enumDescriptor.annotations).enumDefault()?.let {
            enumValueByEnumName[it]
        } ?: -1
    }

    override fun decodeFloat(): Float {
        return when (val v = fieldValue()) {
            is Float -> v
            null -> throw SerializationException("Cannot decode <null> as a Float")
            else -> throw SerializationException("Unsupported type for Float ${v.javaClass}")
        }
    }

    override fun decodeInt(): Int {
        return when (val v = fieldValue()) {
            is Int -> v
            null -> throw SerializationException("Cannot decode <null> as a Int")
            else -> throw SerializationException("Unsupported type for Int ${v.javaClass}")
        }
    }

    override fun decodeShort(): Short {
        return when (val v = fieldValue()) {
            is Short -> v
            is Int -> v.toShort()
            null -> throw SerializationException("Cannot decode <null> as a Short")
            else -> throw SerializationException("Unsupported type for Short ${v.javaClass}")
        }
    }

    override fun decodeLong(): Long {
        return when (val v = fieldValue()) {
            is Long -> v
            is Int -> v.toLong()
            null -> throw SerializationException("Cannot decode <null> as a Long")
            else -> throw SerializationException("Unsupported type for Long [is ${v.javaClass}]")
        }
    }

    override fun decodeDouble(): Double {
        return when (val v = fieldValue()) {
            is Double -> v
            is Float -> v.toDouble()
            null -> throw SerializationException("Cannot decode <null> as a Double")
            else -> throw SerializationException("Unsupported type for Double ${v.javaClass}")
        }
    }

    override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        currentIndex++
        return if (currentIndex < descriptor.elementsCount) currentIndex else CompositeDecoder.DECODE_DONE
    }
}

