package com.github.avrokotlin.avro4k.internal.encoder.direct

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.internal.EncodingWorkflow
import com.github.avrokotlin.avro4k.internal.encoder.ReorderingCompositeEncoder
import kotlinx.serialization.SerializationException
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
    val encodingWorkflow = avro.recordResolver.resolveFields(schema, descriptor).encoding
    when (encodingWorkflow) {
        is EncodingWorkflow.ExactMatch -> return RecordExactDirectEncoder(schema, avro, binaryEncoder)
        is EncodingWorkflow.ContiguousWithSkips -> return RecordSkippingDirectEncoder(encodingWorkflow.fieldsToSkip, schema, avro, binaryEncoder)
        is EncodingWorkflow.NonContiguous -> return ReorderingCompositeEncoder(
            schema.fields.size,
            RecordNonContiguousDirectEncoder(
                encodingWorkflow.descriptorToWriterFieldIndex,
                schema,
                avro,
                binaryEncoder
            )
        ) { _, index ->
            encodingWorkflow.descriptorToWriterFieldIndex[index]
        }

        is EncodingWorkflow.MissingWriterFields -> throw SerializationException("Invalid encoding workflow")
    }
}

private class RecordNonContiguousDirectEncoder(
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

private class RecordSkippingDirectEncoder(
    private val skippedElements: BooleanArray,
    private val schema: Schema,
    avro: Avro,
    binaryEncoder: org.apache.avro.io.Encoder,
) : AbstractAvroDirectEncoder(avro, binaryEncoder) {
    override lateinit var currentWriterSchema: Schema

    override fun encodeElement(
        descriptor: SerialDescriptor,
        index: Int,
    ): Boolean {
        if (skippedElements[index]) {
            return false
        }
        super.encodeElement(descriptor, index)
        currentWriterSchema = schema.fields[index].schema()
        return true
    }
}

private class RecordExactDirectEncoder(
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