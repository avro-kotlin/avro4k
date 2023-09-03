package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.AvroConfiguration
import com.github.avrokotlin.avro4k.getAvroFullName
import com.github.avrokotlin.avro4k.isListOfBytes
import com.github.avrokotlin.avro4k.schema.getTypeNamed
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.descriptors.PolymorphicKind
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.SerialKind
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.encoding.CompositeEncoder
import kotlinx.serialization.encoding.Encoder
import org.apache.avro.AvroRuntimeException
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericData.EnumSymbol
import org.apache.avro.generic.GenericFixed
import org.apache.avro.util.Utf8
import java.math.BigDecimal
import java.nio.ByteBuffer


@OptIn(ExperimentalSerializationApi::class)
abstract class AbstractAvroEncoder : Encoder, NativeAvroEncoder {
    final override lateinit var currentResolvedSchema: Schema
        private set

    abstract val currentUnresolvedSchema: Schema


    abstract val configuration: AvroConfiguration

    protected abstract fun encodeNativeValue(value: Any?)

    //region primitive encode methods

    final override fun encodeBytes(value: ByteBuffer) =
        encodeNativeValue(value)

    final override fun encodeFixed(value: GenericFixed) =
        encodeNativeValue(value)

    final override fun encodeNull() {
        // todo verify schema.isNullable, this is the only case where the path is not using encodeSerializableValue
        encodeNativeValue(null)
    }

    final override fun encodeBoolean(value: Boolean): Unit =
        encodeNativeValue(value)

    final override fun encodeByte(value: Byte) =
        encodeNativeValue(value.toInt())

    final override fun encodeShort(value: Short) =
        encodeNativeValue(value.toInt())

    final override fun encodeInt(value: Int): Unit =
        encodeNativeValue(value)

    final override fun encodeLong(value: Long): Unit =
        encodeNativeValue(value)

    final override fun encodeFloat(value: Float): Unit =
        encodeNativeValue(value)

    final override fun encodeDouble(value: Double): Unit =
        encodeNativeValue(value)

    final override fun encodeEnum(enumDescriptor: SerialDescriptor, index: Int) =
        encodeNativeValue(GenericData.get().createEnum(enumDescriptor.getElementName(index), currentResolvedSchema) as EnumSymbol)

    final override fun encodeChar(value: Char) =
        encodeNativeValue(value.code)

    final override fun encodeString(value: String) {
        val nativeValue = when (currentResolvedSchema.type) {
            Schema.Type.FIXED -> {
                val size = value.toByteArray().size
                if (size > currentResolvedSchema.fixedSize)
                    throw AvroRuntimeException("Cannot write string with $size bytes to fixed type of size ${currentResolvedSchema.fixedSize}")
                // the array passed in must be padded to size
                val bytes = ByteBuffer.allocate(currentResolvedSchema.fixedSize).put(value.toByteArray()).array()
                GenericData.get().createFixed(null, bytes, currentResolvedSchema)
            }

            Schema.Type.BYTES -> ByteBuffer.wrap(value.toByteArray())
            else -> Utf8(value)
        }
        encodeNativeValue(nativeValue)
    }

    final override fun encodeInline(descriptor: SerialDescriptor): Encoder =
        this

    //endregion

    //region structure encode methods

    private fun typeError(schema: Schema, descriptor: SerialDescriptor) = SerializationException("Cannot write schema type of ${schema.type} for descriptor $descriptor")

    override fun beginCollection(descriptor: SerialDescriptor, collectionSize: Int): CompositeEncoder = when (descriptor.kind) {
        StructureKind.LIST -> beginList(descriptor, descriptor.getElementDescriptor(0), collectionSize)
        StructureKind.MAP -> beginMap(collectionSize)
        else -> throw SerializationException("beginCollection expected descriptor kind as a MAP or LIST, but had ${descriptor.kind}. Descriptor: $descriptor")
    }

    private fun beginList(listDescriptor: SerialDescriptor, elementDescriptor: SerialDescriptor, collectionSize: Int): CompositeEncoder = when (currentResolvedSchema.type) {
        Schema.Type.ARRAY -> ListEncoder(currentResolvedSchema, collectionSize, serializersModule, configuration) { encodeNativeValue(it) }
        Schema.Type.BYTES -> when {
            elementDescriptor.kind == PrimitiveKind.BYTE -> BytesEncoder(collectionSize, serializersModule) { encodeNativeValue(it) }
            else -> throw typeError(currentResolvedSchema, listDescriptor)
        }

        Schema.Type.FIXED -> when {
            elementDescriptor.kind == PrimitiveKind.BYTE -> ZeroPaddedBytesEncoder(currentResolvedSchema, collectionSize, serializersModule) { encodeNativeValue(it) }
            else -> throw typeError(currentResolvedSchema, listDescriptor)
        }

        else -> throw SerializationException("beginList has been called with a non-compatible schema type. Expected ARRAY${if (elementDescriptor.kind == PrimitiveKind.BYTE) ", BYTES or FIXED" else ""}. Actual schema: $currentResolvedSchema")
    }

    private fun beginMap(collectionSize: Int) = when (currentResolvedSchema.type) {
        Schema.Type.MAP -> MapEncoder(currentResolvedSchema, collectionSize, serializersModule, configuration) { encodeNativeValue(it) }
        else -> throw SerializationException("beginMap has been called with a non-compatible schema type. Expected MAP. Actual schema: $currentResolvedSchema")
    }

    override fun beginStructure(descriptor: SerialDescriptor): CompositeEncoder {
        return when (descriptor.kind) {
            StructureKind.CLASS,
            StructureKind.OBJECT -> RecordEncoder(currentResolvedSchema, serializersModule, configuration) { encodeNativeValue(it) }

            is PolymorphicKind -> AvroPolymorphicEncoder()
            else -> throw SerializationException(".beginStructure was called on a non-structure type [$descriptor]")
        }
    }

    internal fun resolveElementSchema(descriptor: SerialDescriptor, index: Int, isValueNull: Boolean) {
        currentResolvedSchema = doResolveElementSchema(descriptor, index, isValueNull)
    }

    internal open fun doResolveElementSchema(descriptor: SerialDescriptor, index: Int, isValueNull: Boolean): Schema {
        return doResolveSchema(descriptor.getElementDescriptor(index), isValueNull)
    }

    internal fun resolveSchema(descriptor: SerialDescriptor, isValueNull: Boolean) {
        currentResolvedSchema = doResolveSchema(descriptor, isValueNull)
    }

    internal open fun doResolveSchema(descriptor: SerialDescriptor, isValueNull: Boolean): Schema {
        if (descriptor.isInline) {
            doResolveSchema(descriptor.getElementDescriptor(0), isValueNull)
        }
        return if (isValueNull) {
            if (currentUnresolvedSchema.isNullable) {
                Schema.create(Schema.Type.NULL)
            } else {
                throw SerializationException("Value is null while the schema don't allow it. Actual unresolved schema: $currentUnresolvedSchema")
            }
        } else {
            currentUnresolvedSchema.resolveIfUnion(descriptor)
        }
    }

    //endregion

    //region helpers

    inner class AvroPolymorphicEncoder : PolymorphicEncoder(this@AbstractAvroEncoder.serializersModule) {
        override fun encodeSerialName(polymorphicTypeDescriptor: SerialDescriptor, serialName: String) {
            // Not encoding serialName, apache avro library is doing it if necessary
        }

        override fun <T> encodeValue(serializer: SerializationStrategy<T>, value: T) {
            resolveSchema(serializer.descriptor, value == null)
            this@AbstractAvroEncoder.encodeSerializableValue(serializer, value)
        }
    }

    /**
     * Coming from [org.apache.avro.generic.GenericData.getSchemaName]
     */
    @ExperimentalSerializationApi
    private fun SerialDescriptor.getSchemaName(): String {
        if (isListOfBytes()) {
            return Schema.Type.BYTES.getName()
        }
        // startsWith because if type nullable, then it ends with "?"
        if (serialName.startsWith(BigDecimal::class.qualifiedName!!)) // todo change BigDecimalSerializer kind to LIST of BYTE
            return Schema.Type.BYTES.getName()
        return when (kind) {
            PrimitiveKind.BOOLEAN -> Schema.Type.BOOLEAN.getName()
            PrimitiveKind.DOUBLE -> Schema.Type.DOUBLE.getName()
            PrimitiveKind.FLOAT -> Schema.Type.FLOAT.getName()
            PrimitiveKind.BYTE,
            PrimitiveKind.SHORT,
            PrimitiveKind.CHAR,
            PrimitiveKind.INT -> Schema.Type.INT.getName()

            PrimitiveKind.LONG -> Schema.Type.LONG.getName()
            PrimitiveKind.STRING -> Schema.Type.STRING.getName()
            StructureKind.LIST -> Schema.Type.ARRAY.getName()
            StructureKind.MAP -> Schema.Type.MAP.getName()
            SerialKind.ENUM -> getAvroFullName(configuration.namingStrategy)
            StructureKind.CLASS, StructureKind.OBJECT -> getAvroFullName(configuration.namingStrategy)
            SerialKind.CONTEXTUAL, is PolymorphicKind -> throw SerializationException("getSchemaName should be called on an already resolved descriptor (not a contextual or polymorphic). Actual descriptor: $this")
        }
    }

    @ExperimentalSerializationApi
    internal open fun Schema.resolveIfUnion(descriptor: SerialDescriptor): Schema {
        if (this.type == Schema.Type.UNION
                && descriptor.kind !is PolymorphicKind // for this case, it is just after handled by AvroPolymorphicEncoder
        ) {
            return doResolve(descriptor)
        }
        return this
    }

    @ExperimentalSerializationApi
    private fun Schema.doResolve(descriptor: SerialDescriptor): Schema {
        return this.getTypeNamed(descriptor.getSchemaName())
                ?: throw AvroRuntimeException("Unable to encode with descriptor $descriptor: no schema found in union $this")
    }

    //endregion
}

