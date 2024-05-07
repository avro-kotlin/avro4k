package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.internal.BadEncodedValueError
import com.github.avrokotlin.avro4k.internal.nonNullSerialName
import com.github.avrokotlin.avro4k.internal.toIntExact
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.descriptors.PolymorphicKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.encoding.CompositeEncoder
import kotlinx.serialization.internal.TaggedEncoder
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericFixed
import java.nio.ByteBuffer

@OptIn(InternalSerializationApi::class)
internal abstract class AvroTaggedEncoder<Tag : Any?> : TaggedEncoder<Tag>(), AvroEncoder {
    abstract val avro: Avro
    abstract val Tag.writerSchema: Schema

    abstract override fun encodeTaggedNull(tag: Tag)

    override val serializersModule: SerializersModule
        get() = avro.serializersModule

    override val currentWriterSchema: Schema
        get() = currentTag.writerSchema

    override fun <T> encodeSerializableValue(
        serializer: SerializationStrategy<T>,
        value: T,
    ) {
        if (currentWriterSchema.type == Schema.Type.BYTES ||
            currentWriterSchema.type == Schema.Type.UNION && currentWriterSchema.types.any { it.type == Schema.Type.BYTES }
        ) {
            when (value) {
                is ByteArray -> encodeBytes(value)
                is ByteBuffer -> encodeBytes(value)
                else -> super<TaggedEncoder>.encodeSerializableValue(serializer, value)
            }
        } else {
            super<TaggedEncoder>.encodeSerializableValue(serializer, value)
        }
    }

    override fun beginStructure(descriptor: SerialDescriptor): CompositeEncoder {
        return when (descriptor.kind) {
            StructureKind.CLASS,
            StructureKind.OBJECT,
            ->
                encodeResolvingUnion(
                    currentTag,
                    { BadEncodedValueError(null, currentTag.writerSchema, Schema.Type.RECORD) }
                ) { schema ->
                    if (schema.type == Schema.Type.RECORD &&
                        (schema.fullName == descriptor.nonNullSerialName || schema.aliases.any { it == descriptor.nonNullSerialName })
                    ) {
                        RecordEncoder(avro, descriptor, schema) { encodeTaggedValue(currentTag, it) }
                    } else {
                        null
                    }
                }

            is PolymorphicKind ->
                PolymorphicEncoder(avro, currentTag.writerSchema) {
                    encodeTaggedValue(currentTag, it)
                }

            else -> throw SerializationException("Unsupported structure kind: $descriptor")
        }
    }

    override fun beginCollection(
        descriptor: SerialDescriptor,
        collectionSize: Int,
    ): CompositeEncoder {
        return when (descriptor.kind) {
            StructureKind.LIST ->
                encodeResolvingUnion(
                    currentTag,
                    { BadEncodedValueError(null, currentTag.writerSchema, Schema.Type.ARRAY, Schema.Type.BYTES, Schema.Type.FIXED) }
                ) { schema ->
                    when (schema.type) {
                        Schema.Type.ARRAY -> ArrayEncoder(avro, collectionSize, schema) { encodeTaggedValue(currentTag, it) }
                        Schema.Type.BYTES -> BytesEncoder(avro, collectionSize) { encodeTaggedValue(currentTag, it) }
                        Schema.Type.FIXED -> FixedEncoder(avro, collectionSize, schema) { encodeTaggedValue(currentTag, it) }
                        else -> null
                    }
                }

            StructureKind.MAP ->
                encodeResolvingUnion(
                    currentTag,
                    { BadEncodedValueError(null, currentTag.writerSchema, Schema.Type.MAP) }
                ) { schema ->
                    when (schema.type) {
                        Schema.Type.MAP -> MapEncoder(avro, collectionSize, schema) { encodeTaggedValue(currentTag, it) }
                        else -> null
                    }
                }

            else -> throw SerializationException("Unsupported collection kind: $descriptor")
        }
    }

    override fun encodeBytes(value: ByteBuffer) {
        val tag = currentTag
        encodeResolvingUnion(
            tag,
            { BadEncodedValueError(value, tag.writerSchema, Schema.Type.STRING, Schema.Type.BYTES, Schema.Type.FIXED) }
        ) { schema ->
            when (schema.type) {
                Schema.Type.BYTES -> encodeTaggedValue(tag, value)
                Schema.Type.FIXED -> encodeTaggedValue(tag, value.array().toPaddedGenericFixed(schema, endPadded = false))
                Schema.Type.STRING -> encodeTaggedValue(tag, value.array().decodeToString())
                else -> null
            }
        }
    }

    override fun encodeBytes(value: ByteArray) {
        val tag = currentTag
        encodeResolvingUnion(
            tag,
            { BadEncodedValueError(value, tag.writerSchema, Schema.Type.STRING, Schema.Type.BYTES, Schema.Type.FIXED) }
        ) { schema ->
            when (schema.type) {
                Schema.Type.BYTES -> encodeTaggedValue(tag, ByteBuffer.wrap(value))
                Schema.Type.FIXED -> encodeTaggedValue(tag, value.toPaddedGenericFixed(schema, endPadded = false))
                Schema.Type.STRING -> encodeTaggedValue(tag, value.decodeToString())
                else -> null
            }
        }
    }

    override fun encodeFixed(value: GenericFixed) {
        val tag = currentTag
        encodeResolvingUnion(
            tag,
            { BadEncodedValueError(value, tag.writerSchema, Schema.Type.STRING, Schema.Type.BYTES, Schema.Type.FIXED) }
        ) { schema ->
            when (schema.type) {
                Schema.Type.FIXED ->
                    when (schema.fullName) {
                        value.schema.fullName -> encodeTaggedValue(tag, value)
                        else -> null
                    }

                Schema.Type.BYTES -> encodeTaggedValue(tag, ByteBuffer.wrap(value.bytes()))
                Schema.Type.STRING -> encodeTaggedValue(tag, value.bytes().decodeToString())
                else -> null
            }
        }
    }

    override fun encodeFixed(value: ByteArray) {
        val tag = currentTag
        encodeResolvingUnion(
            tag,
            { BadEncodedValueError(value, tag.writerSchema, Schema.Type.STRING, Schema.Type.BYTES, Schema.Type.FIXED) }
        ) { schema ->
            when (schema.type) {
                Schema.Type.FIXED -> encodeTaggedValue(tag, value.toPaddedGenericFixed(schema, endPadded = false))
                Schema.Type.BYTES -> encodeTaggedValue(tag, ByteBuffer.wrap(value))
                Schema.Type.STRING -> encodeTaggedValue(tag, value.decodeToString())
                else -> null
            }
        }
    }

    override fun encodeTaggedBoolean(
        tag: Tag,
        value: Boolean,
    ) {
        encodeResolvingUnion(
            tag,
            { BadEncodedValueError(value, tag.writerSchema, Schema.Type.BOOLEAN, Schema.Type.STRING) }
        ) { schema ->
            when (schema.type) {
                Schema.Type.BOOLEAN -> encodeTaggedValue(tag, value)
                Schema.Type.STRING -> encodeTaggedValue(tag, value.toString())
                else -> null
            }
        }
    }

    override fun encodeTaggedByte(
        tag: Tag,
        value: Byte,
    ) {
        encodeTaggedInt(tag, value.toInt())
    }

    override fun encodeTaggedShort(
        tag: Tag,
        value: Short,
    ) {
        encodeTaggedInt(tag, value.toInt())
    }

    override fun encodeTaggedInt(
        tag: Tag,
        value: Int,
    ) {
        encodeResolvingUnion(
            tag,
            { BadEncodedValueError(value, tag.writerSchema, Schema.Type.LONG, Schema.Type.INT, Schema.Type.FLOAT, Schema.Type.DOUBLE, Schema.Type.STRING) }
        ) { schema ->
            when (schema.type) {
                Schema.Type.INT -> encodeTaggedValue(tag, value)
                Schema.Type.LONG -> encodeTaggedValue(tag, value.toLong())
                Schema.Type.FLOAT -> encodeTaggedValue(tag, value.toFloat())
                Schema.Type.DOUBLE -> encodeTaggedValue(tag, value.toDouble())
                Schema.Type.STRING -> encodeTaggedValue(tag, value.toString())
                else -> null
            }
        }
    }

    override fun encodeTaggedLong(
        tag: Tag,
        value: Long,
    ) {
        encodeResolvingUnion(
            tag,
            { BadEncodedValueError(value, tag.writerSchema, Schema.Type.LONG, Schema.Type.INT, Schema.Type.FLOAT, Schema.Type.DOUBLE, Schema.Type.STRING) }
        ) { schema ->
            when (schema.type) {
                Schema.Type.LONG -> encodeTaggedValue(tag, value)
                Schema.Type.INT -> encodeTaggedValue(tag, value.toIntExact())
                Schema.Type.FLOAT -> encodeTaggedValue(tag, value.toFloat())
                Schema.Type.DOUBLE -> encodeTaggedValue(tag, value.toDouble())
                Schema.Type.STRING -> encodeTaggedValue(tag, value.toString())
                else -> null
            }
        }
    }

    override fun encodeTaggedFloat(
        tag: Tag,
        value: Float,
    ) {
        encodeResolvingUnion(
            tag,
            { BadEncodedValueError(value, tag.writerSchema, Schema.Type.STRING, Schema.Type.DOUBLE, Schema.Type.FLOAT) }
        ) { schema ->
            when (schema.type) {
                Schema.Type.FLOAT -> encodeTaggedValue(tag, value)
                Schema.Type.DOUBLE -> encodeTaggedValue(tag, value.toDouble())
                Schema.Type.STRING -> encodeTaggedValue(tag, value.toString())
                else -> null
            }
        }
    }

    override fun encodeTaggedDouble(
        tag: Tag,
        value: Double,
    ) {
        encodeResolvingUnion(
            tag,
            { BadEncodedValueError(value, tag.writerSchema, Schema.Type.STRING, Schema.Type.DOUBLE) }
        ) { schema ->
            when (schema.type) {
                Schema.Type.DOUBLE -> encodeTaggedValue(tag, value)
                Schema.Type.STRING -> encodeTaggedValue(tag, value.toString())
                else -> null
            }
        }
    }

    override fun encodeTaggedChar(
        tag: Tag,
        value: Char,
    ) {
        encodeResolvingUnion(
            tag,
            { BadEncodedValueError(value, tag.writerSchema, Schema.Type.INT, Schema.Type.STRING) }
        ) { schema ->
            when (schema.type) {
                Schema.Type.INT -> encodeTaggedValue(tag, value.code)
                Schema.Type.STRING -> encodeTaggedValue(tag, value.toString())
                else -> null
            }
        }
    }

    override fun encodeTaggedString(
        tag: Tag,
        value: String,
    ) {
        encodeResolvingUnion(
            tag,
            { BadEncodedValueError(value, tag.writerSchema, Schema.Type.STRING, Schema.Type.BYTES, Schema.Type.FIXED, Schema.Type.ENUM) }
        ) { schema ->
            when (schema.type) {
                Schema.Type.STRING -> encodeTaggedValue(tag, value)
                Schema.Type.BYTES -> encodeTaggedValue(tag, value.encodeToByteArray())
                Schema.Type.FIXED -> encodeTaggedValue(tag, value.encodeToByteArray().toPaddedGenericFixed(schema, endPadded = true))
                Schema.Type.ENUM -> encodeTaggedValue(tag, GenericData.EnumSymbol(schema, value))
                else -> null
            }
        }
    }

    override fun encodeTaggedEnum(
        tag: Tag,
        enumDescriptor: SerialDescriptor,
        ordinal: Int,
    ) {
        /*
        We allow enums as ENUM (must match the descriptor's full name), STRING or UNION.
        For UNION, we look for an enum with the descriptor's full name, otherwise a string.
         */
        val value = enumDescriptor.getElementName(ordinal)

        encodeResolvingUnion(
            tag,
            { BadEncodedValueError(value, tag.writerSchema, Schema.Type.STRING, Schema.Type.ENUM) }
        ) { schema ->
            when (schema.type) {
                Schema.Type.STRING -> encodeTaggedValue(tag, value)
                Schema.Type.ENUM ->
                    when (schema.fullName) {
                        enumDescriptor.nonNullSerialName -> encodeTaggedValue(tag, GenericData.EnumSymbol(schema, value))
                        else ->
                            schema.aliases.firstOrNull { it == enumDescriptor.nonNullSerialName }?.let {
                                encodeTaggedValue(tag, GenericData.EnumSymbol(schema, value))
                            }
                    }

                else -> null
            }
        }
    }

    private inline fun <T : Any> encodeResolvingUnion(
        tag: Tag,
        error: () -> Throwable,
        resolver: (Schema) -> T?,
    ): T {
        val schema = tag.writerSchema
        return if (schema.type == Schema.Type.UNION) {
            schema.types.firstNotNullOfOrNull(resolver)
        } else {
            resolver(schema)
        } ?: throw error()
    }
}

private fun ByteArray.toPaddedGenericFixed(
    schema: Schema,
    endPadded: Boolean,
): GenericFixed {
    if (size > schema.fixedSize) {
        throw SerializationException("Actual byte array size $size is greater than schema fixed size $schema")
    }
    val padSize = schema.fixedSize - size
    return GenericData.Fixed(
        schema,
        if (padSize > 0) {
            if (endPadded) {
                this + ByteArray(padSize)
            } else {
                ByteArray(padSize) + this
            }
        } else {
            this
        }
    )
}