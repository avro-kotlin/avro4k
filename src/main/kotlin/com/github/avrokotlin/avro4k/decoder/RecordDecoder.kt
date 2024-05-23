package com.github.avrokotlin.avro4k.decoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.internal.DecodedNullError
import com.github.avrokotlin.avro4k.internal.ElementDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.CompositeDecoder
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord

internal class RecordDecoder(
    private val record: IndexedRecord,
    private val descriptor: SerialDescriptor,
    override val avro: Avro,
) : AbstractAvroDecoder() {
    // from descriptor element index to schema field
    private val fields = avro.recordResolver.resolveFields(record.schema, descriptor)
    private lateinit var currentElement: ElementDescriptor
    private var currentElementIndex = 0

    override val currentWriterSchema: Schema
        get() = currentElement.writerFieldSchema

    override fun decodeNotNullMark() = decodeNullableValue() != null

    override fun decodeValue(): Any {
        return decodeNullableValue() ?: throw DecodedNullError(descriptor, currentElementIndex)
    }

    private fun decodeNullableValue(): Any? {
        return currentElement.writerFieldIndex?.let { record.get(it) } ?: currentElement.readerDefaultValue
    }

    override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        while (currentElementIndex < fields.size) {
            val field = fields[currentElementIndex]
            if (field != null) {
                currentElement = field
                return currentElementIndex++
            }
            currentElementIndex++
        }
        return CompositeDecoder.DECODE_DONE
    }
}