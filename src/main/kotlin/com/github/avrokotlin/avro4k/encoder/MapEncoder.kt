package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.Avro
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import org.apache.avro.Schema

private val STRING_SCHEMA = Schema.create(Schema.Type.STRING)

internal class MapEncoder(
    override val avro: Avro,
    mapSize: Int,
    private val schema: Schema,
    private val onEncoded: (Map<String, Any?>) -> Unit,
) : AvroTaggedEncoder<MapEncoder.MapTag>() {
    init {
        schema.ensureTypeOf(Schema.Type.MAP)
    }

    private val entries: MutableList<Pair<String, Any?>> = ArrayList(mapSize)
    private lateinit var currentKey: String

    override fun endEncode(descriptor: SerialDescriptor) {
        onEncoded(entries.associate { it.first to it.second })
    }

    override fun SerialDescriptor.getTag(index: Int) =
        if (index % 2 == 0) {
            MapTag.key()
        } else {
            MapTag.value(schema.valueType)
        }

    override val MapTag.writerSchema: Schema
        get() = schema

    override fun encodeTaggedValue(
        tag: MapTag,
        value: Any,
    ) {
        if (tag.isKey) {
            currentKey = value.toString()
        } else {
            entries.add(currentKey to value)
        }
    }

    override fun encodeTaggedNull(tag: MapTag) {
        if (tag.isKey) {
            throw SerializationException("Map key cannot be null")
        }
        entries.add(currentKey to null)
    }

    data class MapTag(val isKey: Boolean, val schema: Schema) {
        companion object {
            fun key() = MapTag(true, STRING_SCHEMA)

            fun value(schema: Schema) = MapTag(false, schema)
        }
    }
}