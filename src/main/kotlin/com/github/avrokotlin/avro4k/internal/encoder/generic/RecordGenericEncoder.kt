package com.github.avrokotlin.avro4k.internal.encoder.generic

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.ListRecord
import com.github.avrokotlin.avro4k.internal.EncodingStep
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

internal class RecordGenericEncoder(
    override val avro: Avro,
    descriptor: SerialDescriptor,
    private val schema: Schema,
    private val onEncoded: (GenericRecord) -> Unit,
) : AbstractAvroGenericEncoder() {
    private val fieldValues: Array<Any?> = Array(schema.fields.size) { null }

    private val classDescriptor = avro.recordResolver.resolveFields(schema, descriptor)
    private lateinit var currentField: Schema.Field

    override lateinit var currentWriterSchema: Schema

    override fun encodeElement(
        descriptor: SerialDescriptor,
        index: Int,
    ): Boolean {
        super.encodeElement(descriptor, index)
        return when (val step = classDescriptor.encodingSteps[index]) {
            is EncodingStep.SerializeWriterField -> {
                val field = schema.fields[step.writerFieldIndex]
                currentField = field
                currentWriterSchema = field.schema()
                true
            }

            is EncodingStep.IgnoreElement -> {
                false
            }

            is EncodingStep.MissingWriterFieldFailure -> {
                throw SerializationException("No serializable element found for writer field ${step.writerFieldIndex} in schema $schema")
            }
        }
    }

    override fun endStructure(descriptor: SerialDescriptor) {
        onEncoded(ListRecord(schema, fieldValues.asList()))
    }

    override fun encodeValue(value: Any) {
        fieldValues[currentField.pos()] = value
    }

    override fun encodeNull() {
        fieldValues[currentField.pos()] = null
    }
}