package com.github.avrokotlin.avro4k.internal.encoder.generic

import com.github.avrokotlin.avro4k.Avro
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import org.apache.avro.Schema

private val STRING_SCHEMA = Schema.create(Schema.Type.STRING)

internal class MapGenericEncoder(
    override val avro: Avro,
    mapSize: Int,
    private val schema: Schema,
    private val onEncoded: (Map<String, Any?>) -> Unit,
) : AbstractAvroGenericEncoder() {
    private val entries: MutableList<Pair<String, Any?>> = ArrayList(mapSize)
    private var currentKey: String? = null

    override lateinit var currentWriterSchema: Schema

    override fun encodeElement(
        descriptor: SerialDescriptor,
        index: Int,
    ): Boolean {
        super.encodeElement(descriptor, index)
        currentWriterSchema =
            if (index % 2 == 0) {
                currentKey = null
                STRING_SCHEMA
            } else {
                schema.valueType
            }
        return true
    }

    override fun endStructure(descriptor: SerialDescriptor) {
        onEncoded(entries.associate { it.first to it.second })
    }

    override fun encodeValue(value: Any) {
        val key = currentKey
        if (key == null) {
            currentKey = value.toString()
        } else {
            entries.add(key to value)
        }
    }

    override fun encodeNull() {
        val key = currentKey ?: throw SerializationException("Map key cannot be null")
        entries.add(key to null)
    }
}