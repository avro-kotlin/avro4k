package com.github.avrokotlin.avro4k.kafka.confluent

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroDecoder
import com.github.avrokotlin.avro4k.AvroEncoder
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.MissingFieldsEncodingException
import com.github.avrokotlin.avro4k.serializer.AvroSerializer
import com.github.avrokotlin.avro4k.serializer.SchemaSupplierContext
import com.github.avrokotlin.avro4k.serializer.SerialDescriptorWithAvroSchemaDelegate
import com.github.avrokotlin.avro4k.trySelectNamedSchema
import io.confluent.kafka.schemaregistry.utils.BoundedConcurrentHashMap
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.builtins.ByteArraySerializer
import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.builtins.MapSerializer
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.SerialKind
import kotlinx.serialization.descriptors.buildClassSerialDescriptor
import kotlinx.serialization.descriptors.buildSerialDescriptor
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.encoding.decodeStructure
import kotlinx.serialization.encoding.encodeStructure
import kotlinx.serialization.serializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericContainer
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericEnumSymbol
import org.apache.avro.generic.GenericFixed
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.util.Utf8
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Serdes.WrapperSerde
import java.nio.ByteBuffer


/**
 * A Serde that can serialize and deserialize any type supported by Avro4k, based on the confluent's schema registry.
 *
 * Enums are deserialized as [GenericEnumSymbol], records as [GenericRecord], and fixed as [GenericFixed].
 * Enums can be serialized as [GenericEnumSymbol] or as a string, records as [GenericRecord], and fixed as [GenericFixed] or as a byte array.
 *
 * If it is needed to configure the [Avro] instance but is restricted to an empty constructor (like as a default serde through properties in kafka streams which is instantiated through reflection),
 * you can extend this class and provide your own [Avro] instance, or handle the configuration by overriding the [configure] method.
 *
 * To have a specific type to be serialized and deserialized known at compile time, use [Avro4kSerde].
 * To serialize and deserialize any type but using the concrete class for enums, records, fixed and other explicit class in the schema properties, use [ReflectAvro4kKafkaSerde].
 */
@ExperimentalAvro4kApi
public open class GenericAvro4kKafkaSerde(avro: Avro = Avro) : WrapperSerde<Any>(GenericAvro4kKafkaSerializer(avro), GenericAvro4kKafkaDeserializer(avro))

@ExperimentalAvro4kApi
public class GenericAvro4kKafkaSerializer(avro: Avro = Avro) : AbstractAvro4kSerializer<Any?>(avro) {
    private val recordDescriptorCache = BoundedConcurrentHashMap<Schema, SerialDescriptor>()

    override fun getSchema(data: Any?): Schema {
        if (data == null) {
            throw UnsupportedOperationException("The encoding workflow should natively handle null values")
        }
        if (data is GenericContainer) {
            return data.schema
        }
        if (data is ByteBuffer) {
            return Schema.create(Schema.Type.BYTES)
        }
        if (data is Utf8) {
            return Schema.create(Schema.Type.STRING)
        }
        val serializer = try {
            avro.serializersModule.serializer(data::class, emptyList(), false)
        } catch (e: Exception) {
            throw SerializationException(
                "Cannot find serializer for type ${data::class}, " +
                        "please provide a schema explicitly through a ${GenericContainer::class.qualifiedName} " +
                        "or register the corresponding KSerializer in Avro serializers module",
                e
            )
        }
        return avro.schema(serializer.descriptor)
    }

    @Suppress("UNCHECKED_CAST")
    override fun getSerializer(data: Any?): SerializationStrategy<Any?> {
        return when (data) {
            null -> NullSerializer
            is GenericFixed -> GenericFixedSerializer
            is GenericEnumSymbol<*> -> GenericEnumSerializer
            is GenericValue -> ExplicitSchemaGenericContainerSerializer(getSerializer(data.value) as KSerializer<Any?>)
            is IndexedRecord -> GenericRecordSerializer()
            is ByteBuffer -> ByteBufferSerializer
            is Utf8 -> Utf8Serializer
            is Iterable<*> -> ListSerializer(DeferredAnySerializer())
            is Map<*, *> -> MapSerializer(DeferredAnySerializer(), DeferredAnySerializer())
            else -> avro.serializersModule.serializer(data::class.java)
        } as SerializationStrategy<Any?>
    }

    private class ExplicitSchemaGenericContainerSerializer(
        private val valueSerializer: KSerializer<Any?>,
    ) : SerializationStrategy<GenericValue> {
        override val descriptor: SerialDescriptor
            get() = valueSerializer.descriptor

        override fun serialize(encoder: Encoder, value: GenericValue) {
            encoder.encodeNullableSerializableValue(valueSerializer, value.value)
        }
    }

    private object NullSerializer : SerializationStrategy<Any?> {
        @OptIn(InternalSerializationApi::class)
        override val descriptor: SerialDescriptor = buildSerialDescriptor("null", SerialKind.CONTEXTUAL)

        override fun serialize(encoder: Encoder, value: Any?) {
            throw UnsupportedOperationException("This should not be called, as null encoding is handled before")
        }
    }

    private inner class DeferredAnySerializer : JustAvroSerializer<Any>("any") {
        override fun serializeAvro(encoder: AvroEncoder, value: Any) {
            encoder.encodeSerializableValue(getSerializer(value), value)
        }
    }

    private inner class GenericRecordSerializer : JustAvroSerializer<IndexedRecord>(IndexedRecord::class.qualifiedName!!) {
        override fun serializeAvro(encoder: AvroEncoder, value: IndexedRecord) {
            val descriptor = recordDescriptorCache.resolveDescriptor(value.schema) {
                // TODO how to handle fields in writerSchema that are not in value's schema ?
                getSerializer(value.getValueOrDefault(it)).descriptor
            }
            encoder.encodeStructure(descriptor) {
                value.schema.fields.forEachIndexed { index, _ ->
                    val fieldValue = value.getValueOrDefault(index)
                    encodeSerializableElement(descriptor, index, getSerializer(fieldValue), fieldValue)
                }
            }
        }
    }

    private fun IndexedRecord.getValueOrDefault(fieldIndex: Int): Any? {
        val field = schema.fields[fieldIndex]
        var fieldValue = this[fieldIndex]
        if (fieldValue == null && !field.schema().isNullable) {
            // If the field is not nullable and the value is null, we need to provide a default value
            if (field.hasDefaultValue()) {
                fieldValue = field.defaultVal()
            } else {
                throw MissingFieldsEncodingException(listOf(field), schema)
            }
        }
        return fieldValue
    }

    private object ByteBufferSerializer : JustAvroSerializer<ByteBuffer>(ByteBuffer::class.qualifiedName!!) {
        override fun serializeAvro(encoder: AvroEncoder, value: ByteBuffer) {
            encoder.encodeBytes(value.getBytes())
        }

        private fun ByteBuffer.getBytes(): ByteArray {
            if (hasArray()) return array()
            val bytes = ByteArray(remaining())
            val pos = position()
            get(bytes)
            position(pos)
            return bytes
        }
    }

    private object Utf8Serializer : JustAvroSerializer<Utf8>(Utf8::class.qualifiedName!!) {
        override fun serializeAvro(encoder: AvroEncoder, value: Utf8) {
            encoder.encodeString(value.toString())
        }
    }

    private abstract class JustAvroSerializer<T>(name: String) : AvroSerializer<T>(name) {
        final override fun deserializeAvro(decoder: AvroDecoder): T {
            throw UnsupportedOperationException("This serializer is only for serialization")
        }

        final override fun getSchema(context: SchemaSupplierContext): Schema {
            throw UnsupportedOperationException("This serializer is only for serialization")
        }
    }
}

@ExperimentalAvro4kApi
public class GenericAvro4kKafkaDeserializer(avro: Avro = Avro) : AbstractAvro4kDeserializer<Any>(avro) {
    private val kSerializerCache = BoundedConcurrentHashMap<Schema, KSerializer<out Any>>()
    private val recordDescriptorCache = BoundedConcurrentHashMap<Schema, SerialDescriptor>()
    private val logicalTypeNameToSerializer: Map<String, KSerializer<out Any>> = avro.serializersModule.collectLogicalTypesMapping()

    override fun getDeserializer(writerSchema: Schema): DeserializationStrategy<Any> {
        return getKSerializer(writerSchema)
    }

    private fun getKSerializer(writerSchema: Schema): KSerializer<out Any> {
        return kSerializerCache.computeIfAbsent(writerSchema) {
            resolveNonNullKSerializer(writerSchema)
        }
    }

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    private fun resolveNonNullKSerializer(writerSchema: Schema): KSerializer<out Any> =
        // Let's try to use the logical type's serializer if any
        writerSchema.logicalType?.name?.let { logicalTypeNameToSerializer[it] }
        // Else infer the serializer from the schema type
            ?: when (writerSchema.type) {
                Schema.Type.NULL -> throw UnsupportedOperationException("$writerSchema schema is not supported, as writing nulls doesn't require a schema")
                Schema.Type.BOOLEAN -> Boolean.serializer()
                Schema.Type.INT -> Int.serializer()
                Schema.Type.LONG -> Long.serializer()
                Schema.Type.FLOAT -> Float.serializer()
                Schema.Type.DOUBLE -> Double.serializer()
                Schema.Type.STRING -> String.serializer()
                Schema.Type.BYTES -> ByteArraySerializer()
                Schema.Type.ARRAY -> ListSerializer(resolveNonNullKSerializer(writerSchema.elementType))
                Schema.Type.MAP -> MapSerializer(String.serializer(), resolveNonNullKSerializer(writerSchema.valueType))

                Schema.Type.FIXED -> GenericFixedSerializer
                Schema.Type.ENUM -> GenericEnumSerializer
                Schema.Type.RECORD -> GenericRecordSerializer()

                Schema.Type.UNION -> {
                    if (writerSchema.types.size == 2) {
                        val nullType = writerSchema.types.indexOfFirst { it.type == Schema.Type.NULL }
                        when (nullType) {
                            0 -> resolveNonNullKSerializer(writerSchema.types[1])
                            1 -> resolveNonNullKSerializer(writerSchema.types[0])
                            else -> GenericUnionDeserializer()
                        }
                    } else {
                        GenericUnionDeserializer()
                    }
                }
            }

    private inner class GenericUnionDeserializer : AvroSerializer<Any>(GenericUnionDeserializer::class.qualifiedName!!) {
        override fun deserializeAvro(decoder: AvroDecoder): Any {
            return decoder.decodeSerializableValue(getDeserializer(decoder.currentWriterSchema))
        }

        override fun getSchema(context: SchemaSupplierContext): Schema {
            throw UnsupportedOperationException("This serializer is only for deserialization")
        }

        override fun serializeAvro(encoder: AvroEncoder, value: Any) {
            throw UnsupportedOperationException("This serializer is only for deserialization")
        }
    }

    private inner class GenericRecordSerializer : AvroSerializer<GenericRecord>(GenericRecord::class.qualifiedName!!) {
        override fun deserializeAvro(decoder: AvroDecoder): GenericRecord {
            val schema = decoder.currentWriterSchema
            val descriptor = recordDescriptorCache.resolveDescriptor(schema) { getKSerializer(schema.fields[it].schema()).descriptor }
            return decoder.decodeStructure(descriptor) {
                val result = GenericData.Record(schema)
                schema.fields.forEachIndexed { index, field ->
                    result.put(index, decodeNullableSerializableElement(descriptor, index, getKSerializer(field.schema())))
                }
                result
            }
        }

        override fun serializeAvro(encoder: AvroEncoder, value: GenericRecord) {
            throw UnsupportedOperationException("This serializer is only for deserialization")
        }

        override fun getSchema(context: SchemaSupplierContext): Schema {
            throw UnsupportedOperationException("This serializer is only for deserialization")
        }
    }
}

private fun BoundedConcurrentHashMap<Schema, SerialDescriptor>.resolveDescriptor(schema: Schema, fieldResolver: (schemaFieldIndex: Int) -> SerialDescriptor): SerialDescriptor =
    computeIfAbsent(schema) {
        buildClassSerialDescriptor(schema.fullName) {
            schema.fields.forEachIndexed { i, field ->
                val fieldDescriptor = fieldResolver(i)
                element(field.name(), SerialDescriptorWithAvroSchemaDelegate(fieldDescriptor) { field.schema() })
            }
        }
    }

private object GenericEnumSerializer : AvroSerializer<GenericEnumSymbol<*>>(GenericEnumSymbol::class.qualifiedName!!) {
    override fun deserializeAvro(decoder: AvroDecoder): GenericEnumSymbol<*> {
        return GenericData.EnumSymbol(decoder.currentWriterSchema, decoder.decodeString())
    }

    override fun serializeAvro(encoder: AvroEncoder, value: GenericEnumSymbol<*>) {
        if (encoder.currentWriterSchema.isUnion) {
            encoder.trySelectNamedSchema(value.schema.fullName, value.schema::getAliases)
            // When unable to determine the type from the enum schema, delegate it to the native encodeString resolver
        }
        encoder.encodeString(value.toString())
    }

    override fun getSchema(context: SchemaSupplierContext): Schema {
        throw UnsupportedOperationException("This serializer is only for deserialization")
    }
}

private object GenericFixedSerializer : AvroSerializer<GenericFixed>(GenericFixed::class.qualifiedName!!) {
    override fun deserializeAvro(decoder: AvroDecoder): GenericFixed {
        return decoder.decodeFixed()
    }

    override fun serializeAvro(encoder: AvroEncoder, value: GenericFixed) {
        if (encoder.currentWriterSchema.isUnion) {
            encoder.trySelectNamedSchema(value.schema.fullName, value.schema::getAliases)
            // When unable to determine the type from the fixed schema, delegate it to the native encodeFixed resolver
        }
        encoder.encodeFixed(value.bytes())
    }

    override fun getSchema(context: SchemaSupplierContext): Schema {
        throw UnsupportedOperationException("This serializer is only for deserialization")
    }
}
