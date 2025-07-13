@file:OptIn(ExperimentalSerializationApi::class)

package com.github.avrokotlin.avro4k.kafka.confluent

import com.github.avrokotlin.avro4k.AvroDecoder
import com.github.avrokotlin.avro4k.AvroEncoder
import com.github.avrokotlin.avro4k.MissingFieldsEncodingException
import com.github.avrokotlin.avro4k.schemaNotFoundInUnionError
import com.github.avrokotlin.avro4k.serializer.AnySerializer
import com.github.avrokotlin.avro4k.serializer.AvroSerializer
import com.github.avrokotlin.avro4k.serializer.SchemaSupplierContext
import com.github.avrokotlin.avro4k.serializer.SerialDescriptorWithStaticAvroSchema
import com.github.avrokotlin.avro4k.trySelectNamedSchema
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.buildClassSerialDescriptor
import kotlinx.serialization.encoding.decodeStructure
import kotlinx.serialization.encoding.encodeStructure
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.serializerOrNull
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericEnumSymbol
import org.apache.avro.generic.GenericFixed
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.specific.SpecificData
import org.apache.avro.util.Utf8
import java.nio.ByteBuffer
import java.util.WeakHashMap


internal object AvroReflectKSerializer : AnySerializer() {
    override fun SerializersModule.preResolveDeserializationStrategy(writerSchema: Schema): DeserializationStrategy<Any>? {
        return writerSchema.getProp(SpecificData.CLASS_PROP)
            ?.let { runCatching { Class.forName(it) }.getOrNull() }
            ?.let { serializerOrNull(it) }
    }

    override fun SerializersModule.inferSerializationStrategyFromNonSerializableType(type: Class<out Any>) = inferSerializationStrategy(type)
    override fun SerializersModule.resolveEnumDeserializationStrategy(writerSchema: Schema) = resolveNamedSchema(writerSchema) ?: GenericEnumKSerializer
    override fun SerializersModule.resolveRecordDeserializationStrategy(writerSchema: Schema) = resolveNamedSchema(writerSchema) ?: GenericRecordKSerializer
    override fun SerializersModule.resolveFixedDeserializationStrategy(writerSchema: Schema) = resolveNamedSchema(writerSchema) ?: GenericFixedKSerializer

    private fun SerializersModule.resolveNamedSchema(writerSchema: Schema): KSerializer<Any>? = iterator {
        yield(writerSchema.fullName)
        yieldAll(writerSchema.aliases)
    }.asSequence()
        .mapNotNull { runCatching { Class.forName(it) }.getOrNull() }
        .firstNotNullOfOrNull { serializerOrNull(it) }
}

internal object GenericAnyKSerializer : AnySerializer() {
    override fun SerializersModule.inferSerializationStrategyFromNonSerializableType(type: Class<out Any>) = inferSerializationStrategy(type)
    override fun SerializersModule.resolveEnumDeserializationStrategy(writerSchema: Schema) = GenericEnumKSerializer
    override fun SerializersModule.resolveFixedDeserializationStrategy(writerSchema: Schema) = GenericFixedKSerializer
    override fun SerializersModule.resolveRecordDeserializationStrategy(writerSchema: Schema) = GenericRecordKSerializer
}

private fun inferSerializationStrategy(type: Class<out Any>): AvroSerializer<out Any>? = when {
    GenericFixed::class.java.isAssignableFrom(type) -> GenericFixedKSerializer
    GenericEnumSymbol::class.java.isAssignableFrom(type) -> GenericEnumKSerializer
    IndexedRecord::class.java.isAssignableFrom(type) -> GenericRecordKSerializer
    ByteBuffer::class.java.isAssignableFrom(type) -> ByteBufferKSerializer
    Utf8::class.java.isAssignableFrom(type) -> Utf8KSerializer
    else -> null
}

internal object GenericEnumKSerializer : AvroSerializer<GenericEnumSymbol<*>>(GenericEnumSymbol::class.qualifiedName!!) {
    override fun deserializeAvro(decoder: AvroDecoder): GenericEnumSymbol<*> {
        return GenericData.EnumSymbol(decoder.currentWriterSchema, decoder.decodeString())
    }

    override fun serializeAvro(encoder: AvroEncoder, value: GenericEnumSymbol<*>) {
        if (encoder.currentWriterSchema.isUnion) {
            encoder.trySelectNamedSchema(value.schema.fullName, value.schema::getAliases)
            // When unable to determine the type from the enum schema, delegate it to the native encodeString resolver instead of raising an error here
        }
        encoder.encodeString(value.toString())
    }

    override fun getSchema(context: SchemaSupplierContext): Schema {
        throw schemaGenerationUnsupportedError()
    }
}

internal object GenericFixedKSerializer : AvroSerializer<GenericFixed>(GenericFixed::class.qualifiedName!!) {
    override fun deserializeAvro(decoder: AvroDecoder): GenericFixed {
        return decoder.decodeFixed()
    }

    override fun serializeAvro(encoder: AvroEncoder, value: GenericFixed) {
        with(encoder) {
            if (currentWriterSchema.isUnion) {
                trySelectNamedSchema(value.schema.fullName, value.schema::getAliases)
                // When unable to determine the type from the fixed schema, delegate it to the native encodeFixed resolver instead of raising an error here
            }
            encodeFixed(value.bytes())
        }
    }

    override fun getSchema(context: SchemaSupplierContext): Schema {
        throw schemaGenerationUnsupportedError()
    }
}

internal object GenericRecordKSerializer : AvroSerializer<IndexedRecord>(IndexedRecord::class.qualifiedName!!) {
    private val cache = WeakHashMap<Schema, SerialDescriptor>()

    override fun serializeAvro(encoder: AvroEncoder, value: IndexedRecord) {
        with(encoder) {
            if (currentWriterSchema.isUnion) {
                trySelectNamedSchema(value.schema) ||
                        throw schemaNotFoundInUnionError(value.schema)
            }
            val descriptor = value.schema.toGenericDescriptor()
            encodeStructure(descriptor) {
                (0 until descriptor.elementsCount).forEach { index ->
                    val fieldValue = value.get(index)
                    if (fieldValue == null && !descriptor.getElementDescriptor(index).isNullable) {
                        // In IndexedRecord, the implementation can miss a field, which in that case is represented by null,
                        // and .get() does not fail when the field is null even if not authorized.
                        // so we need to fail here, or we will get a "cannot write null to a non-nullable field" error later.
                        throw MissingFieldsEncodingException(listOf(value.schema.fields[index]), value.schema)
                    }
                    encodeNullableSerializableElement(descriptor, index, GenericAnyKSerializer.nullable, fieldValue)
                }
            }
        }
    }

    override fun deserializeAvro(decoder: AvroDecoder): IndexedRecord {
        with(decoder) {
            val descriptor = currentWriterSchema.toGenericDescriptor()
            return decodeStructure(descriptor) {
                val result = GenericData.Record(currentWriterSchema)
                (0 until descriptor.elementsCount).forEach { index ->
                    result.put(
                        index,
                        decodeNullableSerializableElement(descriptor, index, GenericAnyKSerializer)
                    )
                }
                result
            }
        }
    }

    private fun Schema.toGenericDescriptor(): SerialDescriptor =
        cache.computeIfAbsent(this) {
            buildClassSerialDescriptor(fullName) {
                fields.forEach { field ->
                    // Let kotlinx serialization know about the field's nullability.
                    val fieldDescriptor =
                        if (field.schema().isNullable) GenericAnyKSerializer.nullable.descriptor
                        else GenericAnyKSerializer.descriptor
                    element(field.name(), SerialDescriptorWithStaticAvroSchema(fieldDescriptor, field.schema()))
                }
            }
        }

    override fun getSchema(context: SchemaSupplierContext): Schema {
        throw schemaGenerationUnsupportedError()
    }
}

internal object Utf8KSerializer : AvroSerializer<Utf8>(Utf8::class.qualifiedName!!) {
    override fun serializeAvro(encoder: AvroEncoder, value: Utf8) {
        encoder.encodeString(value.toString())
    }

    override fun deserializeAvro(decoder: AvroDecoder): Utf8 {
        return Utf8(decoder.decodeString())
    }

    override fun getSchema(context: SchemaSupplierContext): Schema {
        throw schemaGenerationUnsupportedError()
    }
}

internal object ByteBufferKSerializer : AvroSerializer<ByteBuffer>(ByteBuffer::class.qualifiedName!!) {
    override fun serializeAvro(encoder: AvroEncoder, value: ByteBuffer) {
        encoder.encodeBytes(value.readAllBytes())
    }

    private fun ByteBuffer.readAllBytes(): ByteArray {
        if (hasArray()) return array()
        val bytes = ByteArray(remaining())
        val pos = position()
        get(bytes)
        position(pos)
        return bytes
    }

    override fun deserializeAvro(decoder: AvroDecoder): ByteBuffer {
        return ByteBuffer.wrap(decoder.decodeBytes())
    }

    override fun getSchema(context: SchemaSupplierContext): Schema {
        throw schemaGenerationUnsupportedError()
    }
}

private fun KSerializer<*>.schemaGenerationUnsupportedError(): UnsupportedOperationException =
    UnsupportedOperationException("${this::class} cannot be used to get a schema")
