package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.AvroConfiguration
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.CompositeEncoder
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema
import org.apache.avro.util.Utf8


@ExperimentalSerializationApi
class MapEncoder(
        mapSchema: Schema,
        mapSize: Int,
        override val serializersModule: SerializersModule,
        override val configuration: AvroConfiguration,
        private val callback: (Map<Utf8, *>) -> Unit,
) : AvroStructureEncoder() {
    private val map = HashMap<Utf8, Any?>(Math.multiplyExact(mapSize, 2)) // times 2 to prevent the map growing
    private var currentKey: Utf8? = null

    override fun beginCollection(descriptor: SerialDescriptor, collectionSize: Int): CompositeEncoder {
        if (currentKey == null) {
            throw SerializationException("Cannot encode collection for map keys: only allowed to encode primitive types for map keys")
        }
        return super.beginCollection(descriptor, collectionSize)
    }

    override fun beginStructure(descriptor: SerialDescriptor): CompositeEncoder {
        if (currentKey == null) {
            throw SerializationException("Cannot encode structure for map keys: only allowed to encode primitive types for map keys")
        }
        return super.beginStructure(descriptor)
    }

    override fun doResolveElementSchema(descriptor: SerialDescriptor, index: Int, isValueNull: Boolean): Schema =
            if (isKeyElement(index)) {
                STRING_SCHEMA // Avro maps only accept strings as key
            } else {
                super.doResolveElementSchema(descriptor, index, isValueNull)
            }

    override fun encodeNativeValue(value: Any?) {
        if (currentKey == null) {
            currentKey = if (value is Utf8) {
                value
            } else {
                Utf8(value?.toString())
            }
        } else {
            finalizeMapEntry(value)
        }
    }

    private fun finalizeMapEntry(value: Any?) {
        map[Utf8(currentKey)] = value
        currentKey = null
    }

    override fun endStructure(descriptor: SerialDescriptor) {
        callback(map)
    }

    override val currentUnresolvedSchema: Schema = mapSchema.valueType
}

private val STRING_SCHEMA = Schema.create(Schema.Type.STRING)

private fun isKeyElement(index: Int) = index % 2 == 0
