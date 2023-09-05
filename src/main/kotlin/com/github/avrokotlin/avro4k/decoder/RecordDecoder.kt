package com.github.avrokotlin.avro4k.decoder

import com.github.avrokotlin.avro4k.AvroConfiguration
import com.github.avrokotlin.avro4k.getElementAvroName
import com.github.avrokotlin.avro4k.schema.extractNonNull
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

@ExperimentalSerializationApi
class RecordDecoder(
    descriptor: SerialDescriptor,
    private val record: GenericRecord,
    override val serializersModule: SerializersModule,
    override val configuration: AvroConfiguration,
) : AvroStructureDecoder() {

    private var currentRecordFieldIndex = -1
    private val elementsNameToIndex = (0 until descriptor.elementsCount).associateBy { descriptor.getElementAvroName(configuration.namingStrategy, it).name }
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
        val foundElementIndex = elementsNameToIndex[currentField.name()]
        if (foundElementIndex == null && !configuration.ignoreUnknownFields)
            throw SerializationException("Missing field ${currentField.name()} in descriptor ${descriptor}. This means that the decoded schema field is not present in the kotlin class. Try use another schema/kotlin class or define the configuration 'ignoreUnknownFields' to true to stop throwing this exception")
        return (foundElementIndex ?: return decodeElementIndex(descriptor))
    }
}
