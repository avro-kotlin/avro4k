package com.github.avrokotlin.avro4k.decoder

import com.github.avrokotlin.avro4k.Avro
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.CompositeDecoder
import org.apache.avro.Schema

@ExperimentalSerializationApi
class MapDecoder(
    private val schema: Schema,
    map: Map<*, *>,
    override val avro: Avro
) : AvroStructureDecoder() {

    init {
        require(schema.type == Schema.Type.MAP)
    }

    private val entries = map.toList()
    private var index = -1

    override fun decodeNotNullMark() =
        value() != null

    override val currentSchema: Schema
        get() = schema

    override fun decodeAny(): Any =
        value()!!

    private fun value(): Any? = entries[index / 2].let { if (isKeyIndex()) it.first else it.second }

    override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        return if (++index == entries.size * 2) CompositeDecoder.DECODE_DONE else index
    }

    private fun isKeyIndex() = index % 2 == 0
}

