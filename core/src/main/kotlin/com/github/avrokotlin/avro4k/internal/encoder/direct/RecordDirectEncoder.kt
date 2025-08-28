package com.github.avrokotlin.avro4k.internal.encoder.direct

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.MissingFieldsEncodingException
import com.github.avrokotlin.avro4k.internal.EncodingWorkflow
import com.github.avrokotlin.avro4k.internal.encoder.ReorderingCompositeEncoder
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.CompositeEncoder
import org.apache.avro.Schema

@Suppress("FunctionName")
internal fun RecordDirectEncoder(
    descriptor: SerialDescriptor,
    schema: Schema,
    avro: Avro,
    binaryEncoder: org.apache.avro.io.Encoder,
): CompositeEncoder {
    return when (val encodingWorkflow = avro.recordResolver.resolveFields(schema, descriptor).encoding) {
        is EncodingWorkflow.ExactMatch -> RecordContiguousExactEncoder(schema, avro, binaryEncoder)
        is EncodingWorkflow.ContiguousWithSkips -> RecordContiguousSkippingEncoder(encodingWorkflow.fieldsToSkip, schema, avro, binaryEncoder)
        is EncodingWorkflow.NonContiguous ->
            ReorderingCompositeEncoder(
                schema.fields.size,
                RecordNonContiguousEncoder(
                    encodingWorkflow.descriptorToWriterFieldIndex,
                    schema,
                    avro,
                    binaryEncoder
                )
            ) { _, index ->
                encodingWorkflow.descriptorToWriterFieldIndex[index]
            }

        is EncodingWorkflow.MissingWriterFields -> throw MissingFieldsEncodingException(
            schema.fields.filter { it.pos() in encodingWorkflow.missingWriterFields },
            schema,
            descriptor
        )
    }
}

private class RecordNonContiguousEncoder(
    private val descriptorToWriterFieldIndex: IntArray,
    private val schema: Schema,
    avro: Avro,
    binaryEncoder: org.apache.avro.io.Encoder,
) : AbstractAvroDirectEncoder(avro, binaryEncoder) {
    override lateinit var currentWriterSchema: Schema

    override fun encodeElement(
        descriptor: SerialDescriptor,
        index: Int,
    ): Boolean {
        val writerFieldIndex = descriptorToWriterFieldIndex[index]
        if (writerFieldIndex == -1) {
            return false
        }
        super.encodeElement(descriptor, index)
        currentWriterSchema = schema.fields[writerFieldIndex].schema()
        return true
    }
}

private class RecordContiguousSkippingEncoder(
    private val skippedElements: BooleanArray,
    private val schema: Schema,
    avro: Avro,
    binaryEncoder: org.apache.avro.io.Encoder,
) : AbstractAvroDirectEncoder(avro, binaryEncoder) {
    private var nextWriterFieldIndex = 0
    override lateinit var currentWriterSchema: Schema

    override fun encodeElement(
        descriptor: SerialDescriptor,
        index: Int,
    ): Boolean {
        if (skippedElements[index]) {
            return false
        }
        super.encodeElement(descriptor, index)
        currentWriterSchema = schema.fields[nextWriterFieldIndex++].schema()
        return true
    }
}

private class RecordContiguousExactEncoder(
    private val schema: Schema,
    avro: Avro,
    binaryEncoder: org.apache.avro.io.Encoder,
) : AbstractAvroDirectEncoder(avro, binaryEncoder) {
    override lateinit var currentWriterSchema: Schema

    override fun encodeElement(
        descriptor: SerialDescriptor,
        index: Int,
    ): Boolean {
        super.encodeElement(descriptor, index)
        currentWriterSchema = schema.fields[index].schema()
        return true
    }
}