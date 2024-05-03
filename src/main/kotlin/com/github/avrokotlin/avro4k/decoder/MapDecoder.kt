package com.github.avrokotlin.avro4k.decoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.internal.DecodedNullError
import com.github.avrokotlin.avro4k.internal.IllegalIndexedAccessError
import kotlinx.serialization.descriptors.SerialDescriptor
import org.apache.avro.Schema

internal class MapDecoder(
    private val map: Map<CharSequence, Any?>,
    private val writerSchema: Schema,
    override val avro: Avro,
) : AvroTaggedDecoder<MapDecoder.MapTag>() {
    private val iterator = map.iterator()
    private lateinit var currentEntry: Map.Entry<CharSequence, Any?>
    private var polledEntry = false

    override val MapTag.writerSchema: Schema
        get() = this@MapDecoder.writerSchema.valueType

    override fun SerialDescriptor.getTag(index: Int): MapTag {
        return if (index % 2 == 0) {
            MapTag.key()
        } else {
            MapTag.value(writerSchema.valueType)
        }
    }

    override fun decodeTaggedNotNullMark(tag: MapTag): Boolean {
        if (tag.isKey) {
            polledEntry = true
            currentEntry = iterator.next()
            return true // key never null
        }
        return currentEntry.value != null
    }

    override fun decodeTaggedValue(tag: MapTag): Any {
        if (tag.isKey) {
            if (!polledEntry) {
                currentEntry = iterator.next()
            }
            return currentEntry.key
        }
        polledEntry = false
        return currentEntry.value ?: throw DecodedNullError()
    }

    override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        throw IllegalIndexedAccessError()
    }

    override fun decodeCollectionSize(descriptor: SerialDescriptor) = map.size

    override fun decodeSequentially() = true

    data class MapTag(val isKey: Boolean, val schema: Schema) {
        companion object {
            fun key() = MapTag(true, STRING_SCHEMA)

            fun value(schema: Schema) = MapTag(false, schema)
        }
    }

    companion object {
        private val STRING_SCHEMA = Schema.create(Schema.Type.STRING)
    }
}