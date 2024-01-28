package com.github.avrokotlin.avro4k.encoder

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.AbstractEncoder
import kotlinx.serialization.encoding.CompositeEncoder
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema
import org.apache.avro.generic.GenericFixed
import org.apache.avro.util.Utf8
import java.nio.ByteBuffer

@ExperimentalSerializationApi
class MapEncoder(
    schema: Schema,
    override val serializersModule: SerializersModule,
    private val callback: (Map<Utf8, *>) -> Unit,
) : AbstractEncoder(),
    CompositeEncoder,
    StructureEncoder {
    private val map = mutableMapOf<Utf8, Any>()
    private var key: Utf8? = null
    private val valueSchema = schema.valueType

    override fun encodeString(value: String) {
        val k = key
        if (k == null) {
            key = Utf8(value)
        } else {
            map[k] = StringToAvroValue.toValue(valueSchema, value)
            key = null
        }
    }

    override fun encodeValue(value: Any) {
        val k = key
        if (k == null) {
            throw SerializationException("Expected key but received value $value")
        } else {
            map[k] = value
            key = null
        }
    }

    override fun endStructure(descriptor: SerialDescriptor) {
        callback(map.toMap())
    }

    override fun encodeByteArray(buffer: ByteBuffer) {
        encodeValue(buffer)
    }

    override fun encodeFixed(fixed: GenericFixed) {
        encodeValue(fixed)
    }

    override fun beginStructure(descriptor: SerialDescriptor): CompositeEncoder {
        return super<StructureEncoder>.beginStructure(descriptor)
    }

    override fun addValue(value: Any) {
        encodeValue(value)
    }

    override fun fieldSchema(): Schema = valueSchema
}