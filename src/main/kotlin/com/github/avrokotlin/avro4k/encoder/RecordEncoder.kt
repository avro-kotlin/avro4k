package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.AvroConfiguration
import com.github.avrokotlin.avro4k.getElementAvroName
import com.github.avrokotlin.avro4k.schema.ensureOfType
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.AvroRuntimeException
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder

@ExperimentalSerializationApi
class RecordEncoder(
        private val recordSchema: Schema,
        override val serializersModule: SerializersModule,
        override val configuration: AvroConfiguration,
        private val callback: (GenericRecord) -> Unit,
) : AvroStructureEncoder() {
    private val builder: GenericRecordBuilder
    private lateinit var currentField: Field

    init {
        recordSchema.ensureOfType(Schema.Type.RECORD)
        builder = GenericRecordBuilder(recordSchema)
    }

    override val currentUnresolvedSchema: Schema get() = currentField.schema()

    override fun encodeNativeValue(value: Any?) {
        builder.set(currentField, value)
    }

    override fun shouldEncodeElement(descriptor: SerialDescriptor, index: Int): Boolean {
        val fieldName = descriptor.getElementAvroName(configuration.namingStrategy, index).name
        val foundField = recordSchema.getField(fieldName)
        if (foundField != null) {
            currentField = foundField
            return true
        }
        throw AvroRuntimeException("Missing field $fieldName in schema $recordSchema. This means that the kotlin class is containing a field that is not present in the given schema. Try use another schema or annotate the original class field ${descriptor.getElementName(index)} with @kotlinx.serialization.Transient to skip it")
    }

    override fun endStructure(descriptor: SerialDescriptor) {
        callback(builder.build())
    }
}
