package com.github.avrokotlin.avro4k.internal.encoder.generic

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.ListRecord
import com.github.avrokotlin.avro4k.internal.EncodingWorkflow
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

    private val encodingWorkflow = avro.recordResolver.resolveFields(schema, descriptor).encoding
    private lateinit var currentField: Schema.Field

    override lateinit var currentWriterSchema: Schema

    override fun encodeElement(
        descriptor: SerialDescriptor,
        index: Int,
    ): Boolean {
        super.encodeElement(descriptor, index)
        val writerFieldIndex =
            when (encodingWorkflow) {
                EncodingWorkflow.ExactMatch -> index

                is EncodingWorkflow.ContiguousWithSkips -> {
                    if (encodingWorkflow.fieldsToSkip[index]) {
                        return false
                    }
                    index
                }

                is EncodingWorkflow.NonContiguous -> {
                    val writerFieldIndex = encodingWorkflow.descriptorToWriterFieldIndex[index]
                    if (writerFieldIndex == -1) {
                        return false
                    }
                    writerFieldIndex
                }

                is EncodingWorkflow.MissingWriterFields -> throw SerializationException("Invalid encoding workflow")
            }
        val field = schema.fields[writerFieldIndex]
        currentField = field
        currentWriterSchema = field.schema()
        return true
    }

    override fun endStructure(descriptor: SerialDescriptor) {
        onEncoded(ListRecord(schema, fieldValues.asList()))
    }

    override fun encodeValue(value: Any) {
        fieldValues[currentField.pos()] = value
    }

    override fun encodeNullUnchecked() {
        fieldValues[currentField.pos()] = null
    }
}