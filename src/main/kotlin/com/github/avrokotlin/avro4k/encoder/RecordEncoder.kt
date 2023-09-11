package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.MutableListRecord
import com.github.avrokotlin.avro4k.schema.ensureOfType
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.avro.generic.GenericRecord

@ExperimentalSerializationApi
class RecordEncoder(
    private val recordSchema: Schema,
    override val avro: Avro,
    private val callback: (GenericRecord) -> Unit,
) : AvroStructureEncoder() {
    private val builder = MutableListRecord(recordSchema)
    private lateinit var currentField: Field

    init {
        recordSchema.ensureOfType(Schema.Type.RECORD)
    }

    override val currentUnresolvedSchema: Schema get() = currentField.schema()

    override fun encodeNativeValue(value: Any?) {
        builder[currentField.pos()] = value
    }

    override fun shouldEncodeElement(descriptor: SerialDescriptor, index: Int): Boolean {
        val fieldName = avro.nameResolver.resolveElementName(descriptor, index).name
        val foundField = recordSchema.getField(fieldName)
        if (foundField != null) {
            currentField = foundField
            return true
        }
        if (avro.configuration.ignoreUnknownFields) {
            return false
        }
        throw SerializationException("Missing field $fieldName in schema $recordSchema. This means that the kotlin class is containing a field that is not present in the given schema. Try use another schema, annotate the original class field ${descriptor.getElementName(index)} with @kotlinx.serialization.Transient to skip it, or define the configuration 'ignoreUnknownFields' to true to stop throwing this exception")
    }

    override fun endStructure(descriptor: SerialDescriptor) {
        callback(builder)
    }
}
