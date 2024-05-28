package com.github.avrokotlin.avro4k.internal.decoder.generic

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.internal.DecodedNullError
import com.github.avrokotlin.avro4k.internal.DecodingStep
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.CompositeDecoder
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord

internal class RecordGenericDecoder(
    private val record: IndexedRecord,
    private val descriptor: SerialDescriptor,
    override val avro: Avro,
) : AbstractAvroGenericDecoder() {
    // from descriptor element index to schema field
    private val classDescriptor = avro.recordResolver.resolveFields(record.schema, descriptor)
    private lateinit var currentElement: DecodingStep.ValidatedDecodingStep
    private var nextDecodingStep = 0

    override val currentWriterSchema: Schema
        get() = currentElement.schema

    override fun decodeNotNullMark() = decodeNullableValue() != null

    override fun decodeValue(): Any {
        return decodeNullableValue() ?: throw DecodedNullError(descriptor, currentElement.elementIndex)
    }

    private fun decodeNullableValue(): Any? {
        return when (val element = currentElement) {
            is DecodingStep.DeserializeWriterField -> record.get(element.writerFieldIndex)
            is DecodingStep.GetDefaultValue -> element.defaultValue
        }
    }

    override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        var field: DecodingStep
        do {
            if (nextDecodingStep == classDescriptor.decodingSteps.size) {
                return CompositeDecoder.DECODE_DONE
            }
            field = classDescriptor.decodingSteps[nextDecodingStep++]
        } while (field !is DecodingStep.ValidatedDecodingStep)
        currentElement = field
        return field.elementIndex
    }
}