package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.internal.toIntExact
import com.github.avrokotlin.avro4k.schema.nonNull
import com.github.avrokotlin.avro4k.schema.nonNullSerialName
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.SerializationException
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

    override fun beginStructure(descriptor: SerialDescriptor): CompositeEncoder {
        val schema = currentTag.writerSchema.nonNull
        return when (descriptor.kind) {
            StructureKind.CLASS,
            StructureKind.OBJECT,
            ->
                avro.unionResolver.tryResolveUnion(schema, descriptor.nonNullSerialName)
                    ?.takeIf { it.type == Schema.Type.RECORD }
                    ?.let { RecordEncoder(avro, descriptor, it) { encodeTaggedValue(currentTag, it) } }
                    ?: throwUnsupportedSchemaType(schema, descriptor)

            is PolymorphicKind ->
                PolymorphicEncoder(avro, schema) {
                    encodeTaggedValue(currentTag, it)
                }

            else -> throw SerializationException("Unsupported structure kind: $descriptor")
        }
    }

    override fun beginCollection(
        descriptor: SerialDescriptor,
        collectionSize: Int,
    ): CompositeEncoder {
        val schema = currentTag.writerSchema.nonNull
        return when (descriptor.kind) {
            StructureKind.LIST ->
                when (schema.type) {
                    Schema.Type.ARRAY -> ArrayEncoder(avro, collectionSize, schema) { encodeTaggedValue(currentTag, it) }
                    Schema.Type.BYTES -> BytesEncoder(avro, collectionSize) { encodeTaggedValue(currentTag, it) }
                    Schema.Type.FIXED -> FixedEncoder(avro, collectionSize, schema) { encodeTaggedValue(currentTag, it) }
                    else -> throwUnsupportedSchemaType(schema, descriptor)
                }

            StructureKind.MAP ->
                when (schema.type) {
                    Schema.Type.MAP -> MapEncoder(avro, collectionSize, schema) { encodeTaggedValue(currentTag, it) }
                    else -> throwUnsupportedSchemaType(schema, descriptor)
                }

            else -> throw SerializationException("Unsupported collection kind: $descriptor")
        }
    }

    private fun throwUnsupportedSchemaType(
        schema: Schema,
        descriptor: SerialDescriptor,
    ): Nothing {
        throw SerializationException("Unsupported schema $schema for ${descriptor.kind} $descriptor")
    }

    override fun encodeBytes(value: ByteBuffer) {
        encodeTaggedValueResolved<ByteBuffer>(
            currentTag,
            SchemaTypeMatcher.Scalar.BYTES to { value },
            SchemaTypeMatcher.Named.FirstFixed to { value.array().toPaddedGenericFixed(it, endPadded = false) },
            SchemaTypeMatcher.Scalar.STRING to { value.array().decodeToString() }
        )
    }

    override fun encodeBytes(value: ByteArray) {
        encodeTaggedValueResolved<ByteArray>(
            currentTag,
            SchemaTypeMatcher.Scalar.BYTES to { ByteBuffer.wrap(value) },
            SchemaTypeMatcher.Named.FirstFixed to { value.toPaddedGenericFixed(it, endPadded = false) },
            SchemaTypeMatcher.Scalar.STRING to { value.decodeToString() }
        )
    }

    override fun encodeFixed(value: GenericFixed) {
        encodeTaggedValueResolved<GenericFixed>(
            currentTag,
            SchemaTypeMatcher.Named.Fixed(value.schema.fullName) to { value },
            SchemaTypeMatcher.Scalar.BYTES to { ByteBuffer.wrap(value.bytes()) },
            SchemaTypeMatcher.Scalar.STRING to { value.bytes().decodeToString() }
        )
    }

    override fun encodeFixed(value: ByteArray) {
        encodeTaggedValueResolved<ByteArray>(
            currentTag,
            SchemaTypeMatcher.Named.FirstFixed to { value.toPaddedGenericFixed(it, endPadded = false) },
            SchemaTypeMatcher.Scalar.BYTES to { ByteBuffer.wrap(value) },
            SchemaTypeMatcher.Scalar.STRING to { value.decodeToString() }
        )
    }

    override fun encodeTaggedBoolean(
        tag: Tag,
        value: Boolean,
    ) {
        encodeTaggedValueResolved<Boolean>(
            tag,
            SchemaTypeMatcher.Scalar.BOOLEAN to { value },
            SchemaTypeMatcher.Scalar.STRING to { value.toString() }
        )
    }

    override fun encodeTaggedByte(
        tag: Tag,
        value: Byte,
    ) {
        encodeTaggedValueResolved<Byte>(
            tag,
            SchemaTypeMatcher.Scalar.INT to { value.toInt() },
            SchemaTypeMatcher.Scalar.LONG to { value.toLong() },
            SchemaTypeMatcher.Scalar.STRING to { value.toString() },
            SchemaTypeMatcher.Scalar.DOUBLE to { value.toDouble() },
            SchemaTypeMatcher.Scalar.FLOAT to { value.toFloat() }
        )
    }

    override fun encodeTaggedShort(
        tag: Tag,
        value: Short,
    ) {
        encodeTaggedValueResolved<Short>(
            tag,
            SchemaTypeMatcher.Scalar.INT to { value.toInt() },
            SchemaTypeMatcher.Scalar.LONG to { value.toLong() },
            SchemaTypeMatcher.Scalar.STRING to { value.toString() },
            SchemaTypeMatcher.Scalar.DOUBLE to { value.toDouble() },
            SchemaTypeMatcher.Scalar.FLOAT to { value.toFloat() }
        )
    }

    override fun encodeTaggedInt(
        tag: Tag,
        value: Int,
    ) {
        encodeTaggedValueResolved<Int>(
            tag,
            SchemaTypeMatcher.Scalar.INT to { value },
            SchemaTypeMatcher.Scalar.LONG to { value.toLong() },
            SchemaTypeMatcher.Scalar.STRING to { value.toString() },
            SchemaTypeMatcher.Scalar.DOUBLE to { value.toDouble() },
            SchemaTypeMatcher.Scalar.FLOAT to { value.toFloat() }
        )
    }

    override fun encodeTaggedLong(
        tag: Tag,
        value: Long,
    ) {
        encodeTaggedValueResolved<Long>(
            tag,
            SchemaTypeMatcher.Scalar.LONG to { value },
            SchemaTypeMatcher.Scalar.INT to { value.toIntExact() },
            SchemaTypeMatcher.Scalar.STRING to { value.toString() },
            SchemaTypeMatcher.Scalar.DOUBLE to { value.toDouble() },
            SchemaTypeMatcher.Scalar.FLOAT to { value.toFloat() }
        )
    }

    override fun encodeTaggedFloat(
        tag: Tag,
        value: Float,
    ) {
        encodeTaggedValueResolved<Float>(
            tag,
            SchemaTypeMatcher.Scalar.FLOAT to { value },
            SchemaTypeMatcher.Scalar.DOUBLE to { value.toDouble() },
            SchemaTypeMatcher.Scalar.STRING to { value.toString() },
            SchemaTypeMatcher.Scalar.INT to { value.toInt() }
        )
    }

    override fun encodeTaggedDouble(
        tag: Tag,
        value: Double,
    ) {
        encodeTaggedValueResolved<Double>(
            tag,
            SchemaTypeMatcher.Scalar.DOUBLE to { value },
            SchemaTypeMatcher.Scalar.FLOAT to { value.toFloat() },
            SchemaTypeMatcher.Scalar.STRING to { value.toString() },
            SchemaTypeMatcher.Scalar.INT to { value.toInt() }
        )
    }

    override fun encodeTaggedChar(
        tag: Tag,
        value: Char,
    ) {
        encodeTaggedValueResolved<Char>(
            tag,
            SchemaTypeMatcher.Scalar.INT to { value.code },
            SchemaTypeMatcher.Scalar.STRING to { value.toString() }
        )
    }

    override fun encodeTaggedString(
        tag: Tag,
        value: String,
    ) {
        encodeTaggedValueResolved<String>(
            tag,
            SchemaTypeMatcher.Scalar.STRING to { value },
            SchemaTypeMatcher.Scalar.BYTES to { value.encodeToByteArray() },
            SchemaTypeMatcher.Named.FirstFixed to { value.encodeToByteArray().toPaddedGenericFixed(it, endPadded = true) },
            SchemaTypeMatcher.Named.FirstEnum to { GenericData.EnumSymbol(it, value) }
        )
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

        encodeTaggedValueResolved(
            tag,
            SchemaTypeMatcher.Named.Enum(enumDescriptor.nonNullSerialName) to { GenericData.EnumSymbol(it, value) },
            SchemaTypeMatcher.Scalar.STRING to { value },
            kotlinTypeName = enumDescriptor.serialName
        )
    }

    private inline fun <reified T : Any> encodeTaggedValueResolved(
        tag: Tag,
        vararg encoders: Pair<SchemaTypeMatcher, (Schema) -> Any>,
    ) = encodeTaggedValueResolved(tag, *encoders, kotlinTypeName = T::class.qualifiedName!!)

    override fun encodeValueResolved(
        vararg encoders: Pair<SchemaTypeMatcher, (Schema) -> Any>,
        kotlinTypeName: String,
    ) = encodeTaggedValueResolved(currentTag, *encoders, kotlinTypeName = kotlinTypeName)

    private fun encodeTaggedValueResolved(
        tag: Tag,
        vararg encoders: Pair<SchemaTypeMatcher, (Schema) -> Any>,
        kotlinTypeName: String,
    ) {
        // TODO cache the resolved type from the elementIndex
        //  We have to retrieve the SerialDescriptor and the elementIndex from the tag
        //  to cache the resolved schema given SerialDescriptor, elementIndex and the non-resolved writer schema.
        val schema = tag.writerSchema
        val encoder = hashMapOf(*encoders)

        val valueEncoder =
            schema.toTypeMatchers()
                .firstNotNullOfOrNull { typeMatcher ->
                    encoder[typeMatcher.first]?.let { typeMatcher.second to it }
                }
        if (valueEncoder == null) {
            if (schema.type == Schema.Type.UNION) {
                throw SerializationException(
                    "Expected one of schema types ${encoder.keys} but no compatible schema type found " +
                        "for encoded kotlin type $kotlinTypeName in union $schema"
                )
            } else {
                throw SerializationException(
                    "The kotlin type $kotlinTypeName expected to be encoded as ${encoder.keys} but was $schema"
                )
            }
        }
        encodeTaggedValue(tag, valueEncoder.second(valueEncoder.first))
    }
}

@Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
private fun Schema.toTypeMatchers(): Sequence<Pair<SchemaTypeMatcher, Schema>> =
    when (type) {
        Schema.Type.BOOLEAN -> sequenceOf(SchemaTypeMatcher.Scalar.BOOLEAN to this)
        Schema.Type.INT -> sequenceOf(SchemaTypeMatcher.Scalar.INT to this)
        Schema.Type.LONG -> sequenceOf(SchemaTypeMatcher.Scalar.LONG to this)
        Schema.Type.FLOAT -> sequenceOf(SchemaTypeMatcher.Scalar.FLOAT to this)
        Schema.Type.DOUBLE -> sequenceOf(SchemaTypeMatcher.Scalar.DOUBLE to this)
        Schema.Type.STRING -> sequenceOf(SchemaTypeMatcher.Scalar.STRING to this)
        Schema.Type.BYTES -> sequenceOf(SchemaTypeMatcher.Scalar.BYTES to this)
        Schema.Type.NULL -> sequenceOf(SchemaTypeMatcher.Scalar.NULL to this)
        Schema.Type.FIXED ->
            sequenceOf(SchemaTypeMatcher.Named.Fixed(fullName) to this) +
                aliases.map { SchemaTypeMatcher.Named.Fixed(it) to this } +
                sequenceOf(SchemaTypeMatcher.Named.FirstFixed to this)

        Schema.Type.ENUM ->
            sequenceOf(SchemaTypeMatcher.Named.Enum(fullName) to this) +
                aliases.map { SchemaTypeMatcher.Named.Enum(it) to this } +
                sequenceOf(SchemaTypeMatcher.Named.FirstEnum to this)

        Schema.Type.RECORD ->
            sequenceOf(SchemaTypeMatcher.Named.Record(fullName) to this) +
                aliases.map { SchemaTypeMatcher.Named.Record(it) to this }

        Schema.Type.ARRAY -> sequenceOf(SchemaTypeMatcher.FirstArray to this)
        Schema.Type.MAP -> sequenceOf(SchemaTypeMatcher.FirstMap to this)
        Schema.Type.UNION -> types.asSequence().flatMap { it.toTypeMatchers() }
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