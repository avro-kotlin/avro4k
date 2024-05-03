package com.github.avrokotlin.avro4k.decoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.internal.DecodedNullError
import com.github.avrokotlin.avro4k.internal.ElementDescriptor
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.CompositeDecoder
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord

internal class RecordDecoder(
    private val record: IndexedRecord,
    private val descriptor: SerialDescriptor,
    override val avro: Avro,
) : AvroTaggedDecoder<ElementDescriptor>() {
    // from descriptor element index to schema field
    private val fields = avro.recordResolver.resolveFields(record.schema, descriptor)
    private var currentIndex = 0

    override val ElementDescriptor.writerSchema: Schema
        get() = writerFieldSchema

    override fun decodeTaggedNotNullMark(tag: ElementDescriptor) = decodeTaggedNullableValue(tag) != null

    override fun decodeTaggedValue(tag: ElementDescriptor): Any {
        return decodeTaggedNullableValue(tag) ?: throw DecodedNullError(descriptor, tag.elementIndex)
    }

    private fun decodeTaggedNullableValue(tag: ElementDescriptor): Any? {
        return tag.writerFieldIndex?.let { record.get(it) } ?: tag.readerDefaultValue
    }

    override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        while (currentIndex < fields.size) {
            val field = fields[currentIndex++]
            if (field != null) {
                return field.elementIndex
            }
        }
        return CompositeDecoder.DECODE_DONE
    }

    override fun SerialDescriptor.getTag(index: Int) = fields[index] ?: throw SerializationException("An optional field should not be decoded")
}