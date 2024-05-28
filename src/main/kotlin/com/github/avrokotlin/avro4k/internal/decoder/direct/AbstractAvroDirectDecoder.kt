package com.github.avrokotlin.avro4k.internal.decoder.direct

import com.github.avrokotlin.avro4k.AnyValueDecoder
import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.BooleanValueDecoder
import com.github.avrokotlin.avro4k.CharValueDecoder
import com.github.avrokotlin.avro4k.DoubleValueDecoder
import com.github.avrokotlin.avro4k.FloatValueDecoder
import com.github.avrokotlin.avro4k.IntValueDecoder
import com.github.avrokotlin.avro4k.LongValueDecoder
import com.github.avrokotlin.avro4k.UnionDecoder
import com.github.avrokotlin.avro4k.decodeResolvingAny
import com.github.avrokotlin.avro4k.decodeResolvingBoolean
import com.github.avrokotlin.avro4k.decodeResolvingChar
import com.github.avrokotlin.avro4k.decodeResolvingDouble
import com.github.avrokotlin.avro4k.decodeResolvingFloat
import com.github.avrokotlin.avro4k.decodeResolvingInt
import com.github.avrokotlin.avro4k.decodeResolvingLong
import com.github.avrokotlin.avro4k.internal.IllegalIndexedAccessError
import com.github.avrokotlin.avro4k.internal.UnexpectedDecodeSchemaError
import com.github.avrokotlin.avro4k.internal.decoder.direct.AbstractAvroDirectDecoder.SizeGetter
import com.github.avrokotlin.avro4k.internal.getElementIndexNullable
import com.github.avrokotlin.avro4k.internal.isFullNameOrAliasMatch
import com.github.avrokotlin.avro4k.internal.nonNullSerialName
import com.github.avrokotlin.avro4k.internal.toByteExact
import com.github.avrokotlin.avro4k.internal.toFloatExact
import com.github.avrokotlin.avro4k.internal.toIntExact
import com.github.avrokotlin.avro4k.internal.toShortExact
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.builtins.ByteArraySerializer
import kotlinx.serialization.descriptors.PolymorphicKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.encoding.AbstractDecoder
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.internal.AbstractCollectionSerializer
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericFixed

internal abstract class AbstractAvroDirectDecoder(
    protected val avro: Avro,
    protected val binaryDecoder: org.apache.avro.io.Decoder,
) : AbstractInterceptingDecoder(), UnionDecoder {
    abstract override var currentWriterSchema: Schema
    private var currentlyReadingCollection: Boolean = false

    override val serializersModule: SerializersModule
        get() = avro.serializersModule

    @Deprecated("Do not use it for direct encoding")
    final override fun decodeValue(): Any {
        throw UnsupportedOperationException("Direct decoding doesn't support decoding generic values")
    }

    @OptIn(InternalSerializationApi::class)
    override fun <T> decodeSerializableValue(deserializer: DeserializationStrategy<T>): T {
        if (deserializer == ByteArraySerializer()) {
            // fast-path for ByteArray fields, to avoid slow-path with ArrayDirectDecoder
            @Suppress("UNCHECKED_CAST")
            return decodeBytes() as T
        }

        if (deserializer is AbstractCollectionSerializer<*, T, *> && currentWriterSchema.isCollectionOrMap()) {
            var result: T = deserializer.merge(this, null)
            with(result.collectionSizeGetter()) {
                if (result.collectionSize() > 0) {
                    currentlyReadingCollection = true
                    var prevSize = result.collectionSize()
                    while (true) {
                        result = deserializer.merge(this@AbstractAvroDirectDecoder, result)
                        val newSize = result.collectionSize()
                        if (prevSize == newSize) {
                            break
                        }
                        prevSize = newSize
                    }
                    currentlyReadingCollection = false
                }
            }
            return result
        }

        return super<AbstractInterceptingDecoder>.decodeSerializableValue(deserializer)
    }

    private fun Schema.isCollectionOrMap(): Boolean {
        if (isUnion) {
            this.types.forEach {
                if (it.isCollectionOrMap()) {
                    return true
                }
            }
            return false
        }
        return this.type == Schema.Type.ARRAY || this.type == Schema.Type.MAP
    }

    fun interface SizeGetter<T> {
        fun T.collectionSize(): Int
    }

    private fun <T> T.collectionSizeGetter(): SizeGetter<T> {
        return when (this) {
            is Collection<*> -> SizeGetter { size }
            is Map<*, *> -> SizeGetter { size }
            is Array<*> -> SizeGetter { size }
            is BooleanArray -> SizeGetter { size }
            is ByteArray -> SizeGetter { size }
            is ShortArray -> SizeGetter { size }
            is IntArray -> SizeGetter { size }
            is LongArray -> SizeGetter { size }
            is FloatArray -> SizeGetter { size }
            is DoubleArray -> SizeGetter { size }
            is CharArray -> SizeGetter { size }
            else -> throw SerializationException("Unsupported collection type: ${this?.let { it::class }}")
        }
    }

    override fun beginStructure(descriptor: SerialDescriptor): CompositeDecoder {
        return when (descriptor.kind) {
            StructureKind.LIST ->
                decodeResolvingAny({
                    UnexpectedDecodeSchemaError(
                        descriptor.nonNullSerialName,
                        Schema.Type.ARRAY,
                        Schema.Type.BYTES
                    )
                }) {
                    when (it.type) {
                        Schema.Type.ARRAY -> {
                            AnyValueDecoder { ArrayBlockDirectDecoder(it, decodeFirstBlock = !currentlyReadingCollection, avro, binaryDecoder) }
                        }

                        Schema.Type.BYTES -> {
                            AnyValueDecoder { BytesDirectDecoder(avro, binaryDecoder) }
                        }

                        else -> null
                    }
                }

            StructureKind.MAP ->
                decodeResolvingAny({
                    UnexpectedDecodeSchemaError(
                        descriptor.nonNullSerialName,
                        Schema.Type.MAP
                    )
                }) {
                    when (it.type) {
                        Schema.Type.MAP -> {
                            AnyValueDecoder { MapBlockDirectDecoder(it, decodeFirstBlock = !currentlyReadingCollection, avro, binaryDecoder) }
                        }

                        else -> null
                    }
                }

            StructureKind.CLASS, StructureKind.OBJECT ->
                decodeResolvingAny({
                    UnexpectedDecodeSchemaError(
                        descriptor.nonNullSerialName,
                        Schema.Type.RECORD
                    )
                }) {
                    when (it.type) {
                        Schema.Type.RECORD -> {
                            AnyValueDecoder { RecordDirectDecoder(it, descriptor, avro, binaryDecoder) }
                        }

                        else -> null
                    }
                }

            is PolymorphicKind -> PolymorphicDecoder(avro, descriptor, currentWriterSchema, binaryDecoder)
            else -> throw SerializationException("Unsupported descriptor for structure decoding: $descriptor")
        }
    }

    override fun decodeAndResolveUnion() {
        if (currentWriterSchema.isUnion) {
            currentWriterSchema = currentWriterSchema.types[binaryDecoder.readIndex()]
        }
    }

    override fun decodeNotNullMark(): Boolean {
        decodeAndResolveUnion()
        return currentWriterSchema.type != Schema.Type.NULL
    }

    override fun decodeNull(): Nothing? {
        decodeResolvingAny({
            UnexpectedDecodeSchemaError(
                "null",
                Schema.Type.NULL
            )
        }) {
            when (it.type) {
                Schema.Type.NULL -> {
                    AnyValueDecoder { binaryDecoder.readNull() }
                }

                else -> null
            }
        }
        return null
    }

    override fun decodeBoolean(): Boolean {
        return decodeResolvingBoolean({
            UnexpectedDecodeSchemaError(
                "boolean",
                Schema.Type.BOOLEAN,
                Schema.Type.STRING
            )
        }) {
            when (it.type) {
                Schema.Type.BOOLEAN -> {
                    BooleanValueDecoder { binaryDecoder.readBoolean() }
                }

                Schema.Type.STRING -> {
                    BooleanValueDecoder { binaryDecoder.readString().toBooleanStrict() }
                }

                else -> null
            }
        }
    }

    override fun decodeByte(): Byte {
        return decodeInt().toByteExact()
    }

    override fun decodeShort(): Short {
        return decodeInt().toShortExact()
    }

    override fun decodeInt(): Int {
        return decodeResolvingInt({
            UnexpectedDecodeSchemaError(
                "int",
                Schema.Type.INT,
                Schema.Type.LONG,
                Schema.Type.FLOAT,
                Schema.Type.DOUBLE,
                Schema.Type.STRING
            )
        }) {
            when (it.type) {
                Schema.Type.INT -> {
                    IntValueDecoder { binaryDecoder.readInt() }
                }

                Schema.Type.LONG -> {
                    IntValueDecoder { binaryDecoder.readLong().toIntExact() }
                }

                Schema.Type.FLOAT -> {
                    IntValueDecoder { binaryDecoder.readDouble().toInt() }
                }

                Schema.Type.DOUBLE -> {
                    IntValueDecoder { binaryDecoder.readDouble().toInt() }
                }

                Schema.Type.STRING -> {
                    IntValueDecoder { binaryDecoder.readString().toInt() }
                }

                else -> null
            }
        }
    }

    override fun decodeLong(): Long {
        return decodeResolvingLong({
            UnexpectedDecodeSchemaError(
                "long",
                Schema.Type.INT,
                Schema.Type.LONG,
                Schema.Type.FLOAT,
                Schema.Type.DOUBLE,
                Schema.Type.STRING
            )
        }) {
            when (it.type) {
                Schema.Type.INT -> {
                    LongValueDecoder { binaryDecoder.readInt().toLong() }
                }

                Schema.Type.LONG -> {
                    LongValueDecoder { binaryDecoder.readLong() }
                }

                Schema.Type.FLOAT -> {
                    LongValueDecoder { binaryDecoder.readFloat().toLong() }
                }

                Schema.Type.DOUBLE -> {
                    LongValueDecoder { binaryDecoder.readDouble().toLong() }
                }

                Schema.Type.STRING -> {
                    LongValueDecoder { binaryDecoder.readString().toLong() }
                }

                else -> null
            }
        }
    }

    override fun decodeFloat(): Float {
        return decodeResolvingFloat({
            UnexpectedDecodeSchemaError(
                "float",
                Schema.Type.INT,
                Schema.Type.LONG,
                Schema.Type.FLOAT,
                Schema.Type.DOUBLE,
                Schema.Type.STRING
            )
        }) {
            when (it.type) {
                Schema.Type.INT -> {
                    FloatValueDecoder { binaryDecoder.readInt().toFloat() }
                }

                Schema.Type.LONG -> {
                    FloatValueDecoder { binaryDecoder.readLong().toFloat() }
                }

                Schema.Type.FLOAT -> {
                    FloatValueDecoder { binaryDecoder.readFloat() }
                }

                Schema.Type.DOUBLE -> {
                    FloatValueDecoder { binaryDecoder.readDouble().toFloatExact() }
                }

                Schema.Type.STRING -> {
                    FloatValueDecoder { binaryDecoder.readString().toFloat() }
                }

                else -> null
            }
        }
    }

    override fun decodeDouble(): Double {
        return decodeResolvingDouble({
            UnexpectedDecodeSchemaError(
                "double",
                Schema.Type.INT,
                Schema.Type.LONG,
                Schema.Type.FLOAT,
                Schema.Type.DOUBLE,
                Schema.Type.STRING
            )
        }) {
            when (it.type) {
                Schema.Type.INT -> {
                    DoubleValueDecoder { binaryDecoder.readInt().toDouble() }
                }

                Schema.Type.LONG -> {
                    DoubleValueDecoder { binaryDecoder.readLong().toDouble() }
                }

                Schema.Type.FLOAT -> {
                    DoubleValueDecoder { binaryDecoder.readFloat().toDouble() }
                }

                Schema.Type.DOUBLE -> {
                    DoubleValueDecoder { binaryDecoder.readDouble() }
                }

                Schema.Type.STRING -> {
                    DoubleValueDecoder { binaryDecoder.readString().toDouble() }
                }

                else -> null
            }
        }
    }

    override fun decodeChar(): Char {
        return decodeResolvingChar({
            UnexpectedDecodeSchemaError(
                "char",
                Schema.Type.INT,
                Schema.Type.STRING
            )
        }) {
            when (it.type) {
                Schema.Type.INT -> {
                    CharValueDecoder { binaryDecoder.readInt().toChar() }
                }

                Schema.Type.STRING -> {
                    CharValueDecoder { binaryDecoder.readString(null).single() }
                }

                else -> null
            }
        }
    }

    override fun decodeString(): String {
        return decodeResolvingAny({
            UnexpectedDecodeSchemaError(
                "string",
                Schema.Type.STRING,
                Schema.Type.BYTES,
                Schema.Type.FIXED
            )
        }) {
            when (it.type) {
                Schema.Type.STRING,
                Schema.Type.BYTES,
                -> {
                    AnyValueDecoder { binaryDecoder.readString() }
                }

                Schema.Type.FIXED -> {
                    AnyValueDecoder { ByteArray(it.fixedSize).also { buf -> binaryDecoder.readFixed(buf) }.decodeToString() }
                }

                else -> null
            }
        }
    }

    override fun decodeEnum(enumDescriptor: SerialDescriptor): Int {
        return decodeResolvingInt({
            UnexpectedDecodeSchemaError(
                enumDescriptor.nonNullSerialName,
                Schema.Type.ENUM,
                Schema.Type.STRING
            )
        }) {
            when (it.type) {
                Schema.Type.ENUM ->
                    if (it.isFullNameOrAliasMatch(enumDescriptor)) {
                        IntValueDecoder {
                            val enumName = it.enumSymbols[binaryDecoder.readEnum()]
                            enumDescriptor.getElementIndexNullable(enumName)
                                ?: avro.enumResolver.getDefaultValueIndex(enumDescriptor)
                                ?: throw SerializationException(
                                    "Unknown enum symbol name '$enumName' for Enum '${enumDescriptor.serialName}' for writer schema $currentWriterSchema"
                                )
                        }
                    } else {
                        null
                    }

                Schema.Type.STRING -> {
                    IntValueDecoder {
                        val enumSymbol = binaryDecoder.readString()
                        enumDescriptor.getElementIndex(enumSymbol)
                            .takeIf { index -> index >= 0 }
                            ?: avro.enumResolver.getDefaultValueIndex(enumDescriptor)
                            ?: throw SerializationException("Unknown enum symbol '$enumSymbol' for Enum '${enumDescriptor.serialName}'")
                    }
                }

                else -> null
            }
        }
    }

    override fun decodeBytes(): ByteArray {
        return decodeResolvingAny({
            UnexpectedDecodeSchemaError(
                "ByteArray",
                Schema.Type.BYTES,
                Schema.Type.FIXED,
                Schema.Type.STRING
            )
        }) {
            when (it.type) {
                Schema.Type.BYTES -> {
                    AnyValueDecoder { binaryDecoder.readBytes(null).array() }
                }

                Schema.Type.FIXED -> {
                    AnyValueDecoder { ByteArray(it.fixedSize).also { buf -> binaryDecoder.readFixed(buf) } }
                }

                Schema.Type.STRING -> {
                    AnyValueDecoder { binaryDecoder.readString(null).bytes }
                }

                else -> null
            }
        }
    }

    override fun decodeFixed(): GenericFixed {
        return decodeResolvingAny({
            UnexpectedDecodeSchemaError(
                "GenericFixed",
                Schema.Type.BYTES,
                Schema.Type.FIXED
            )
        }) {
            when (it.type) {
                Schema.Type.BYTES -> {
                    AnyValueDecoder { GenericData.Fixed(it, binaryDecoder.readBytes(null).array()) }
                }

                Schema.Type.FIXED -> {
                    AnyValueDecoder { GenericData.Fixed(it, ByteArray(it.fixedSize).also { buf -> binaryDecoder.readFixed(buf) }) }
                }

                else -> null
            }
        }
    }
}

private class PolymorphicDecoder(
    private val avro: Avro,
    private val descriptor: SerialDescriptor,
    private val schema: Schema,
    private val binaryDecoder: org.apache.avro.io.Decoder,
) : AbstractDecoder() {
    override val serializersModule: SerializersModule
        get() = avro.serializersModule

    private lateinit var chosenSchema: Schema

    override fun decodeString(): String {
        chosenSchema =
            if (schema.isUnion) {
                schema.types[binaryDecoder.readIndex()]
            } else {
                schema
            }

        return tryFindSerialName(chosenSchema)
            ?: throw SerializationException("Unknown schema name ${schema.fullName} for polymorphic type ${descriptor.nonNullSerialName}")
    }

    private fun tryFindSerialName(schema: Schema): String? {
        val namesAndAliasesToSerialName = avro.polymorphicResolver.getFullNamesAndAliasesToSerialName(descriptor)
        return namesAndAliasesToSerialName[schema.fullName]
            ?: schema.aliases.firstNotNullOfOrNull { namesAndAliasesToSerialName[it] }
    }

    override fun <T> decodeSerializableValue(deserializer: DeserializationStrategy<T>): T {
        return AvroValueDirectDecoder(chosenSchema, avro, binaryDecoder)
            .decodeSerializableValue(deserializer)
    }

    override fun decodeSequentially() = true

    override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        throw IllegalIndexedAccessError()
    }
}