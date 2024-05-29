package com.github.avrokotlin.avro4k.internal.encoder.direct

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroEncoder
import com.github.avrokotlin.avro4k.UnionEncoder
import com.github.avrokotlin.avro4k.internal.ClassDescriptorForWriterSchema
import com.github.avrokotlin.avro4k.internal.EncodingStep
import kotlinx.serialization.SerializationException
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.AbstractEncoder
import kotlinx.serialization.encoding.CompositeEncoder
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema
import org.apache.avro.generic.GenericFixed
import java.nio.ByteBuffer

@Suppress("FunctionName")
internal fun RecordDirectEncoder(
    classDescriptor: ClassDescriptorForWriterSchema,
    schema: Schema,
    avro: Avro,
    binaryEncoder: org.apache.avro.io.Encoder,
): CompositeEncoder {
    return if (classDescriptor.sequentialEncoding) {
        RecordSequentialDirectEncoder(classDescriptor, schema, avro, binaryEncoder)
    } else {
        RecordBadOrderDirectEncoder(classDescriptor, schema, avro, binaryEncoder)
    }
}

/**
 * Consider that the descriptor elements are in the same order as the schema fields, and all the fields are represented by an element.
 */
private class RecordSequentialDirectEncoder(
    private val classDescriptor: ClassDescriptorForWriterSchema,
    protected val schema: Schema,
    avro: Avro,
    binaryEncoder: org.apache.avro.io.Encoder,
) : AbstractAvroDirectEncoder(avro, binaryEncoder) {
    override lateinit var currentWriterSchema: Schema

    override fun encodeElement(
        descriptor: SerialDescriptor,
        index: Int,
    ): Boolean {
        super.encodeElement(descriptor, index)
        // index == elementIndex == writerFieldIndex, so the written field is already in the good order
        return when (val step = classDescriptor.encodingSteps[index]) {
            is EncodingStep.SerializeWriterField -> {
                currentWriterSchema = schema.fields[step.writerFieldIndex].schema()
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
        if (descriptor.elementsCount < classDescriptor.encodingSteps.size) {
            throw SerializationException("The descriptor is not writing all the expected fields of writer schema. Schema: $schema, descriptor: $descriptor")
        }
    }
}

/**
 * This handles the case where the descriptor elements are not in the same order as the schema fields.
 *
 * First we buffer all the element encodings to the corresponding field indexes, then we encode them for real in the correct order using [RecordSequentialDirectEncoder].
 *
 * Not implementing [UnionEncoder] as all the encoding is delegated to the [RecordSequentialDirectEncoder] which already handles union encoding.
 */
private class RecordBadOrderDirectEncoder(
    private val classDescriptor: ClassDescriptorForWriterSchema,
    private val schema: Schema,
    private val avro: Avro,
    private val binaryEncoder: org.apache.avro.io.Encoder,
) : AbstractEncoder(), AvroEncoder {
    // Each time we encode a field, if the next expected schema field index is not the good one, it is buffered until it's the time to encode it
    private var bufferedFields = Array<BufferedField?>(schema.fields.size) { null }
    private lateinit var encodingStepToBuffer: EncodingStep.SerializeWriterField

    data class BufferedField(
        val step: EncodingStep.SerializeWriterField,
        val encoder: AvroEncoder.() -> Unit,
    )

    override val currentWriterSchema: Schema
        get() = encodingStepToBuffer.schema

    override val serializersModule: SerializersModule
        get() = avro.serializersModule

    override fun encodeElement(
        descriptor: SerialDescriptor,
        index: Int,
    ): Boolean {
        return when (val step = classDescriptor.encodingSteps[index]) {
            is EncodingStep.SerializeWriterField -> {
                encodingStepToBuffer = step
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

    private inline fun bufferEncoding(crossinline encoder: AvroEncoder.() -> Unit) {
        bufferedFields[encodingStepToBuffer.writerFieldIndex] = BufferedField(encodingStepToBuffer) { encoder() }
    }

    override fun endStructure(descriptor: SerialDescriptor) {
        encodeBufferedFields(descriptor)
    }

    private fun encodeBufferedFields(descriptor: SerialDescriptor) {
        val recordEncoder = RecordSequentialDirectEncoder(classDescriptor, schema, avro, binaryEncoder)
        bufferedFields.forEach { fieldToEncode ->
            if (fieldToEncode == null) {
                throw SerializationException("The writer field is missing in the buffered fields, it hasn't been encoded yet")
            }
            // To simulate the behavior of regular element encoding
            // We don't use the return of encodeElement because we know it's always true
            recordEncoder.encodeElement(descriptor, fieldToEncode.step.elementIndex)
            fieldToEncode.encoder(recordEncoder)
        }
    }

    override fun <T> encodeSerializableValue(
        serializer: SerializationStrategy<T>,
        value: T,
    ) {
        bufferEncoding { encodeSerializableValue(serializer, value) }
    }

    override fun encodeNull() {
        bufferEncoding { encodeNull() }
    }

    override fun encodeBytes(value: ByteArray) {
        bufferEncoding { encodeBytes(value) }
    }

    override fun encodeBytes(value: ByteBuffer) {
        bufferEncoding { encodeBytes(value) }
    }

    override fun encodeFixed(value: GenericFixed) {
        bufferEncoding { encodeFixed(value) }
    }

    override fun encodeFixed(value: ByteArray) {
        bufferEncoding { encodeFixed(value) }
    }

    override fun encodeBoolean(value: Boolean) {
        bufferEncoding { encodeBoolean(value) }
    }

    override fun encodeByte(value: Byte) {
        bufferEncoding { encodeByte(value) }
    }

    override fun encodeShort(value: Short) {
        bufferEncoding { encodeShort(value) }
    }

    override fun encodeInt(value: Int) {
        bufferEncoding { encodeInt(value) }
    }

    override fun encodeLong(value: Long) {
        bufferEncoding { encodeLong(value) }
    }

    override fun encodeFloat(value: Float) {
        bufferEncoding { encodeFloat(value) }
    }

    override fun encodeDouble(value: Double) {
        bufferEncoding { encodeDouble(value) }
    }

    override fun encodeChar(value: Char) {
        bufferEncoding { encodeChar(value) }
    }

    override fun encodeString(value: String) {
        bufferEncoding { encodeString(value) }
    }

    override fun encodeEnum(
        enumDescriptor: SerialDescriptor,
        index: Int,
    ) {
        bufferEncoding { encodeEnum(enumDescriptor, index) }
    }
}