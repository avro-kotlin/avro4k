package com.github.avrokotlin.avro4k.decoder

import com.github.avrokotlin.avro4k.AvroInternalConfiguration
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.PolymorphicKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.encoding.AbstractDecoder
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.encoding.CompositeDecoder.Companion.DECODE_DONE
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema
import org.apache.avro.generic.GenericArray
import org.apache.avro.generic.GenericRecord

@ExperimentalSerializationApi
class ListDecoder(
    private val schema: Schema,
    private val array: List<Any?>,
    override val serializersModule: SerializersModule,
    private val configuration: AvroInternalConfiguration,
) : AbstractDecoder(), FieldDecoder {
    init {
        require(schema.type == Schema.Type.ARRAY)
    }

    private var index = -1

    override fun decodeBoolean(): Boolean {
        return array[index] as Boolean
    }

    override fun decodeLong(): Long {
        return array[index] as Long
    }

    override fun decodeString(): String {
        val raw = array[index]
        return StringFromAvroValue.fromValue(raw)
    }

    override fun decodeDouble(): Double {
        return array[index] as Double
    }

    override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        index++
        return if (index < array.size) index else DECODE_DONE
    }

    override fun decodeFloat(): Float {
        return array[index] as Float
    }

    override fun decodeByte(): Byte {
        return array[index] as Byte
    }

    override fun decodeInt(): Int {
        return array[index] as Int
    }

    override fun decodeChar(): Char {
        return array[index] as Char
    }

    override fun decodeAny(): Any? {
        return array[index]
    }

    override fun fieldSchema(): Schema = schema.elementType

    override fun decodeEnum(enumDescriptor: SerialDescriptor): Int {
        val symbol = array[index]!!.toString()
        return (0 until enumDescriptor.elementsCount).find { enumDescriptor.getElementName(it) == symbol } ?: -1
    }

    override fun <T> decodeSerializableValue(deserializer: DeserializationStrategy<T>): T {
        return deserializer.deserialize(this)
    }

    @Suppress("UNCHECKED_CAST")
    override fun beginStructure(descriptor: SerialDescriptor): CompositeDecoder {
        return when (descriptor.kind) {
            StructureKind.CLASS -> RecordDecoder(descriptor, array[index] as GenericRecord, serializersModule, configuration)
            StructureKind.LIST -> ListDecoder(schema.elementType, array[index] as GenericArray<*>, serializersModule, configuration)
            StructureKind.MAP -> MapDecoder(schema.elementType, array[index] as Map<String, *>, serializersModule, configuration)
            PolymorphicKind.SEALED, PolymorphicKind.OPEN -> UnionDecoder(descriptor, array[index] as GenericRecord, serializersModule, configuration)
            else -> throw UnsupportedOperationException("Kind ${descriptor.kind} is currently not supported.")
        }
    }

    override fun decodeCollectionSize(descriptor: SerialDescriptor): Int = array.size
}