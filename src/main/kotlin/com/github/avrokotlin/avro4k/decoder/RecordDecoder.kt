package com.github.avrokotlin.avro4k.decoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.schema.extractNonNull
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.CompositeDecoder
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

@ExperimentalSerializationApi
class RecordDecoder(
    private val record: GenericRecord,
    override val avro: Avro,
) : AvroStructureDecoder() {

    private var currentRecordFieldIndex = -1
    private lateinit var currentField: Schema.Field

    private fun fieldValue(): Any? =
        record.get(currentField.pos())

    override val currentSchema: Schema
        get() =
            currentField.schema().extractNonNull()

    override fun decodeAny(): Any = fieldValue()!!

    override fun decodeNotNullMark(): Boolean {
        return fieldValue() != null
    }

    override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        if (++currentRecordFieldIndex == record.schema.fields.size) {
            return CompositeDecoder.DECODE_DONE
        }
        currentField = record.schema.fields[currentRecordFieldIndex]
        val foundElementIndex = avro.nameResolver.resolveElementIndex(descriptor, currentField.name())
        if (foundElementIndex == null && !avro.configuration.ignoreUnknownFields)
            throw SerializationException("Missing field ${currentField.name()} in descriptor ${descriptor}. This means that the decoded schema field is not present in the kotlin class. Try use another schema/kotlin class or define the configuration 'ignoreUnknownFields' to true to stop throwing this exception")
        return (foundElementIndex ?: return decodeElementIndex(descriptor))
    }
}
