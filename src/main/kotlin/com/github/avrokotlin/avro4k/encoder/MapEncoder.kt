package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.AvroInternalConfiguration
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
    override val configuration: AvroInternalConfiguration,
    private val callback: (Map<Utf8, *>) -> Unit,
) : AbstractEncoder(),
    CompositeEncoder,
    StructureEncoder {
    private val map = mutableMapOf<Utf8, Any?>()
    private var key: String? = null
    private val valueSchema = schema.valueType

    override fun encodeString(value: String) {
        if (key == null) {
            key = value
        } else {
            finalizeMapEntry(StringToAvroValue.toValue(valueSchema, value))
        }
    }

    override fun encodeBoolean(value: Boolean) {
        if (key == null) {
            key = value.toString()
        } else {
            finalizeMapEntry(value)
        }
    }

    override fun encodeByte(value: Byte) {
        if (key == null) {
            key = value.toString()
        } else {
            finalizeMapEntry(value)
        }
    }

    override fun encodeChar(value: Char) {
        if (key == null) {
            key = value.toString()
        } else {
            finalizeMapEntry(value)
        }
    }

    override fun encodeDouble(value: Double) {
        if (key == null) {
            key = value.toString()
        } else {
            finalizeMapEntry(value)
        }
    }

    override fun encodeEnum(
        enumDescriptor: SerialDescriptor,
        index: Int,
    ) {
        val value = enumDescriptor.getElementName(index)
        if (key == null) {
            key = value
        } else {
            finalizeMapEntry(value)
        }
    }

    override fun encodeInt(value: Int) {
        if (key == null) {
            key = value.toString()
        } else {
            finalizeMapEntry(value)
        }
    }

    override fun encodeLong(value: Long) {
        if (key == null) {
            key = value.toString()
        } else {
            finalizeMapEntry(value)
        }
    }

    override fun encodeFloat(value: Float) {
        if (key == null) {
            key = value.toString()
        } else {
            finalizeMapEntry(value)
        }
    }

    override fun encodeShort(value: Short) {
        if (key == null) {
            key = value.toString()
        } else {
            finalizeMapEntry(value)
        }
    }

    override fun encodeValue(value: Any) {
        val k = key
        if (k == null) {
            throw SerializationException("Expected key but received value $value")
        } else {
            finalizeMapEntry(value)
        }
    }

    override fun encodeNull() {
        val k = key
        if (k == null) {
            throw SerializationException("Expected key but received <null>")
        } else {
            finalizeMapEntry(null)
        }
    }

    private fun finalizeMapEntry(value: Any?) {
        map[Utf8(key)] = value
        key = null
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