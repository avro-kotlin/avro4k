package com.github.avrokotlin.avro4k.internal.decoder.direct

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.internal.DecodingStep
import com.github.avrokotlin.avro4k.internal.decoder.generic.AvroValueGenericDecoder
import com.github.avrokotlin.avro4k.internal.nonNullSerialName
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.SerializationException
import kotlinx.serialization.builtins.ByteArraySerializer
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.CompositeDecoder
import org.apache.avro.Schema
import org.apache.avro.generic.GenericFixed
import org.apache.avro.io.Decoder

internal class RecordDirectDecoder(
    private val writerRecordSchema: Schema,
    descriptor: SerialDescriptor,
    avro: Avro,
    binaryDecoder: Decoder,
) : AbstractAvroDirectDecoder(avro, binaryDecoder) {
    // from descriptor element index to schema field. The missing fields are at the end to decode the default values
    private val classDescriptor = avro.recordResolver.resolveFields(writerRecordSchema, descriptor)
    private lateinit var currentDecodingStep: DecodingStep.ValidatedDecodingStep
    private var nextDecodingStepIndex = 0

    override lateinit var currentWriterSchema: Schema

    override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        var field: DecodingStep
        while (true) {
            if (nextDecodingStepIndex == classDescriptor.decoding.size) {
                return CompositeDecoder.DECODE_DONE
            }
            field = classDescriptor.decoding[nextDecodingStepIndex++]
            when (field) {
                is DecodingStep.IgnoreOptionalElement -> {
                    // loop again to ignore the optional element
                }

                is DecodingStep.SkipWriterField -> binaryDecoder.skip(field.schema)
                is DecodingStep.MissingElementValueFailure -> {
                    throw SerializationException(
                        "Reader field '${descriptor.nonNullSerialName}.${descriptor.getElementName(
                            field.elementIndex
                        )}' has no corresponding field in writer schema $writerRecordSchema"
                    )
                }

                is DecodingStep.DeserializeWriterField -> {
                    currentDecodingStep = field
                    currentWriterSchema = field.schema
                    return field.elementIndex
                }

                is DecodingStep.GetDefaultValue -> {
                    currentDecodingStep = field
                    currentWriterSchema = field.schema
                    return field.elementIndex
                }
            }
        }
    }

    private fun <T> decodeDefault(
        element: DecodingStep.GetDefaultValue,
        deserializer: DeserializationStrategy<T>,
    ): T {
        return AvroValueGenericDecoder(avro, element.defaultValue, currentWriterSchema)
            .decodeSerializableValue(deserializer)
    }

    override fun decodeNotNullMark(): Boolean {
        return when (val element = currentDecodingStep) {
            is DecodingStep.DeserializeWriterField -> super.decodeNotNullMark()
            is DecodingStep.GetDefaultValue -> element.defaultValue != null
        }
    }

    override fun decodeNull(): Nothing? {
        return when (val element = currentDecodingStep) {
            is DecodingStep.DeserializeWriterField -> super.decodeNull()
            is DecodingStep.GetDefaultValue -> {
                if (element.defaultValue != null) {
                    // Should not occur as decodeNotNullMark() should be called first
                    throw SerializationException("Trying to decode a null value for a missing field while the default value is not null")
                }
                null
            }
        }
    }

    override fun decodeFixed(): GenericFixed {
        return when (val element = currentDecodingStep) {
            is DecodingStep.DeserializeWriterField -> super.decodeFixed()
            is DecodingStep.GetDefaultValue -> element.defaultValue as GenericFixed
        }
    }

    override fun decodeInt(): Int {
        return when (val element = currentDecodingStep) {
            is DecodingStep.DeserializeWriterField -> super.decodeInt()
            is DecodingStep.GetDefaultValue -> decodeDefault(element, Int.serializer())
        }
    }

    override fun decodeLong(): Long {
        return when (val element = currentDecodingStep) {
            is DecodingStep.DeserializeWriterField -> super.decodeLong()
            is DecodingStep.GetDefaultValue -> decodeDefault(element, Long.serializer())
        }
    }

    override fun decodeBoolean(): Boolean {
        return when (val element = currentDecodingStep) {
            is DecodingStep.DeserializeWriterField -> super.decodeBoolean()
            is DecodingStep.GetDefaultValue -> decodeDefault(element, Boolean.serializer())
        }
    }

    override fun decodeChar(): Char {
        return when (val element = currentDecodingStep) {
            is DecodingStep.DeserializeWriterField -> super.decodeChar()
            is DecodingStep.GetDefaultValue -> decodeDefault(element, Char.serializer())
        }
    }

    override fun decodeString(): String {
        return when (val element = currentDecodingStep) {
            is DecodingStep.DeserializeWriterField -> super.decodeString()
            is DecodingStep.GetDefaultValue -> decodeDefault(element, String.serializer())
        }
    }

    override fun decodeDouble(): Double {
        return when (val element = currentDecodingStep) {
            is DecodingStep.DeserializeWriterField -> super.decodeDouble()
            is DecodingStep.GetDefaultValue -> decodeDefault(element, Double.serializer())
        }
    }

    override fun decodeFloat(): Float {
        return when (val element = currentDecodingStep) {
            is DecodingStep.DeserializeWriterField -> super.decodeFloat()
            is DecodingStep.GetDefaultValue -> decodeDefault(element, Float.serializer())
        }
    }

    override fun <T> decodeSerializableValue(deserializer: DeserializationStrategy<T>): T {
        return when (val element = currentDecodingStep) {
            is DecodingStep.DeserializeWriterField -> super.decodeSerializableValue(deserializer)
            is DecodingStep.GetDefaultValue -> decodeDefault(element, deserializer)
        }
    }

    override fun decodeEnum(enumDescriptor: SerialDescriptor): Int {
        return when (val element = currentDecodingStep) {
            is DecodingStep.DeserializeWriterField -> super.decodeEnum(enumDescriptor)
            is DecodingStep.GetDefaultValue -> decodeDefault(element, Int.serializer())
        }
    }

    override fun decodeBytes(): ByteArray {
        return when (val element = currentDecodingStep) {
            is DecodingStep.DeserializeWriterField -> super.decodeBytes()
            is DecodingStep.GetDefaultValue -> decodeDefault(element, ByteArraySerializer())
        }
    }
}

private fun Decoder.skip(s: Schema) {
    val schema =
        if (s.isUnion) {
            s.types[readIndex()]
        } else {
            s
        }
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    when (schema.type) {
        Schema.Type.BOOLEAN -> readBoolean()
        Schema.Type.INT -> readInt()
        Schema.Type.LONG -> readLong()
        Schema.Type.FLOAT -> readFloat()
        Schema.Type.DOUBLE -> readDouble()
        Schema.Type.STRING -> skipString()
        Schema.Type.BYTES -> skipBytes()
        Schema.Type.FIXED -> skipFixed(schema.fixedSize)
        Schema.Type.ENUM -> readEnum()
        Schema.Type.ARRAY -> {
            var arrayBlockItems: Long = skipArray()
            while (arrayBlockItems > 0L) {
                for (i in 0L until arrayBlockItems) {
                    skip(schema.elementType)
                }
                arrayBlockItems = skipArray()
            }
        }

        Schema.Type.MAP -> {
            var mapBlockItems: Long = skipMap()
            while (mapBlockItems > 0L) {
                for (i in 0L until mapBlockItems) {
                    skipString()
                    skip(schema.valueType)
                }
                mapBlockItems = skipMap()
            }
        }

        Schema.Type.NULL -> readNull()
        Schema.Type.RECORD -> {
            schema.fields.forEach {
                skip(it.schema())
            }
        }

        // Impossible to go to this branch, as the schema is resolved earlier in this method
        Schema.Type.UNION -> throw UnsupportedOperationException("Union type should be already resolved")
    }
}