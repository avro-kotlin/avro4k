package com.github.avrokotlin.avro4k.internal.encoder.generic

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.ListRecord
import com.github.avrokotlin.avro4k.internal.EncodingWorkflow
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.CompositeEncoder
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

@Suppress("FunctionName")
internal fun RecordGenericEncoder(
    descriptor: SerialDescriptor,
    schema: Schema,
    avro: Avro,
    onEncoded: (GenericRecord) -> Unit,
): CompositeEncoder {
    return when (val encodingWorkflow = avro.recordResolver.resolveFields(schema, descriptor).encoding) {
        is EncodingWorkflow.ExactMatch -> RecordContiguousExactEncoder(schema, avro, onEncoded)
        is EncodingWorkflow.ContiguousWithSkips -> RecordContiguousSkippingEncoder(encodingWorkflow.fieldsToSkip, schema, avro, onEncoded)
        is EncodingWorkflow.NonContiguous -> RecordNonContiguousEncoder(encodingWorkflow.descriptorToWriterFieldIndex, schema, avro, onEncoded)
        is EncodingWorkflow.MissingWriterFields -> throw SerializationException(
            "Missing writer fields ${schema.fields.filter { it.pos() in encodingWorkflow.missingWriterFields }}} from the descriptor $descriptor"
        )
    }
}

private class RecordNonContiguousEncoder(
    private val descriptorToWriterFieldIndex: IntArray,
    schema: Schema,
    avro: Avro,
    onEncoded: (GenericRecord) -> Unit,
) : AbstractRecordGenericEncoder(avro, schema, onEncoded) {
    override fun encodeElement(
        descriptor: SerialDescriptor,
        index: Int,
    ): Boolean {
        val writerFieldIndex = descriptorToWriterFieldIndex[index]
        if (writerFieldIndex == -1) {
            return false
        }
        super.encodeElement(descriptor, index)
        setWriterField(writerFieldIndex)
        return true
    }
}

private class RecordContiguousSkippingEncoder(
    private val skippedElements: BooleanArray,
    schema: Schema,
    avro: Avro,
    onEncoded: (GenericRecord) -> Unit,
) : AbstractRecordGenericEncoder(avro, schema, onEncoded) {
    private var nextWriterFieldIndex = 0

    override fun encodeElement(
        descriptor: SerialDescriptor,
        index: Int,
    ): Boolean {
        if (skippedElements[index]) {
            return false
        }
        super.encodeElement(descriptor, index)
        setWriterField(nextWriterFieldIndex++)
        return true
    }
}

private class RecordContiguousExactEncoder(
    schema: Schema,
    avro: Avro,
    onEncoded: (GenericRecord) -> Unit,
) : AbstractRecordGenericEncoder(avro, schema, onEncoded) {
    override fun encodeElement(
        descriptor: SerialDescriptor,
        index: Int,
    ): Boolean {
        super.encodeElement(descriptor, index)
        setWriterField(index)
        return true
    }
}

private abstract class AbstractRecordGenericEncoder(
    override val avro: Avro,
    private val schema: Schema,
    private val onEncoded: (GenericRecord) -> Unit,
) : AbstractAvroGenericEncoder() {
    private val fieldValues: Array<Any?> = Array(schema.fields.size) { null }

    private lateinit var currentField: Schema.Field

    override lateinit var currentWriterSchema: Schema

    protected fun setWriterField(writerFieldIndex: Int) {
        val field = schema.fields[writerFieldIndex]
        currentField = field
        currentWriterSchema = field.schema()
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