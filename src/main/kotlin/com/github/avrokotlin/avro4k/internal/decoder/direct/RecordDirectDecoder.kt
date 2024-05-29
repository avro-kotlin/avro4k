package com.github.avrokotlin.avro4k.internal.decoder.direct

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.internal.DecodingStep
import com.github.avrokotlin.avro4k.internal.decoder.generic.AvroValueGenericDecoder
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
    recordSchema: Schema,
    descriptor: SerialDescriptor,
    avro: Avro,
    binaryDecoder: org.apache.avro.io.Decoder,
) : AbstractAvroDirectDecoder(avro, binaryDecoder) {
    // from descriptor element index to schema field. The missing fields are at the end to decode the default values
    private val classDescriptor = avro.recordResolver.resolveFields(recordSchema, descriptor)
    private lateinit var currentDecodingStep: DecodingStep.ValidatedDecodingStep
    private var nextDecodingStepIndex = 0

    override lateinit var currentWriterSchema: Schema

    override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        var field: DecodingStep
        while (true) {
            if (nextDecodingStepIndex == classDescriptor.decodingSteps.size) {
                return CompositeDecoder.DECODE_DONE
            }
            field = classDescriptor.decodingSteps[nextDecodingStepIndex++]
            when (field) {
                is DecodingStep.IgnoreOptionalElement -> {
                    // loop again to ignore the optional element
                }
                is DecodingStep.SkipWriterField -> binaryDecoder.skip(field.schema)
                is DecodingStep.MissingElementValueFailure -> {
                    throw SerializationException("No writer schema field matching element index ${field.elementIndex} in descriptor $descriptor")
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

    private inline fun <T> decodeDefaultIfMissing(
        deserializer: DeserializationStrategy<T>,
        block: () -> T,
    ): T {
        return when (val element = currentDecodingStep) {
            is DecodingStep.DeserializeWriterField -> block()
            is DecodingStep.GetDefaultValue ->
                AvroValueGenericDecoder(avro, element.defaultValue, currentWriterSchema)
                    .decodeSerializableValue(deserializer)
        }
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
        return decodeDefaultIfMissing(Int.serializer()) {
            super.decodeInt()
        }
    }

    override fun decodeLong(): Long {
        return decodeDefaultIfMissing(Long.serializer()) {
            super.decodeLong()
        }
    }

    override fun decodeBoolean(): Boolean {
        return decodeDefaultIfMissing(Boolean.serializer()) {
            super.decodeBoolean()
        }
    }

    override fun decodeChar(): Char {
        return decodeDefaultIfMissing(Char.serializer()) {
            super.decodeChar()
        }
    }

    override fun decodeString(): String {
        return decodeDefaultIfMissing(String.serializer()) {
            super.decodeString()
        }
    }

    override fun decodeDouble(): Double {
        return decodeDefaultIfMissing(Double.serializer()) {
            super.decodeDouble()
        }
    }

    override fun decodeFloat(): Float {
        return decodeDefaultIfMissing(Float.serializer()) {
            super.decodeFloat()
        }
    }

    override fun <T> decodeSerializableValue(deserializer: DeserializationStrategy<T>): T {
        return decodeDefaultIfMissing(deserializer) {
            super.decodeSerializableValue(deserializer)
        }
    }

    override fun decodeEnum(enumDescriptor: SerialDescriptor): Int {
        return decodeDefaultIfMissing(Int.serializer()) {
            super.decodeEnum(enumDescriptor)
        }
    }

    override fun decodeBytes(): ByteArray {
        return decodeDefaultIfMissing(ByteArraySerializer()) {
            super.decodeBytes()
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
                    skip(schema.elementType)
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

        else -> throw SerializationException("Unsupported schema type for $schema")
    }
}