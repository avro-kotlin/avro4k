package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.ListRecord
import com.github.avrokotlin.avro4k.internal.ElementDescriptor
import com.github.avrokotlin.avro4k.schema.ensureTypeOf
import kotlinx.serialization.SerializationException
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.descriptors.SerialDescriptor
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

internal class RecordEncoder(
    override val avro: Avro,
    descriptor: SerialDescriptor,
    private val schema: Schema,
    private val onEncoded: (GenericRecord) -> Unit,
) : AvroTaggedEncoder<ElementDescriptor>() {
    init {
        schema.ensureTypeOf(Schema.Type.RECORD)
    }

    private val fieldValues: Array<Any?> = Array(schema.fields.size) { null }

    // from descriptor element index to schema field
    private val fields = avro.recordResolver.resolveFields(schema, descriptor)

    override fun <T : Any> encodeNullableSerializableElement(
        descriptor: SerialDescriptor,
        index: Int,
        serializer: SerializationStrategy<T>,
        value: T?,
    ) {
        // Skip data class fields that are not present in the schema
        if (fields[index]?.writerFieldIndex != null) {
            super.encodeNullableSerializableElement(descriptor, index, serializer, value)
        }
    }

    override fun <T> encodeSerializableElement(
        descriptor: SerialDescriptor,
        index: Int,
        serializer: SerializationStrategy<T>,
        value: T,
    ) {
        // Skip data class fields that are not present in the schema
        if (fields[index]?.writerFieldIndex != null) {
            super.encodeSerializableElement(descriptor, index, serializer, value)
        }
    }

    override fun endEncode(descriptor: SerialDescriptor) {
        onEncoded(ListRecord(schema, fieldValues.asList()))
    }

    override fun SerialDescriptor.getTag(index: Int) =
        fields[index] ?: throw SerializationException("An optional kotlin field without corresponding writer field should not be encoded")

    override val ElementDescriptor.writerSchema: Schema
        get() = writerFieldSchema

    override fun encodeTaggedValue(
        tag: ElementDescriptor,
        value: Any,
    ) {
        if (tag.writerFieldIndex != null) {
            fieldValues[tag.writerFieldIndex] = value
        }
    }

    override fun encodeTaggedNull(tag: ElementDescriptor) {
        if (tag.writerFieldIndex != null) {
            fieldValues[tag.writerFieldIndex] = null
        }
    }
}