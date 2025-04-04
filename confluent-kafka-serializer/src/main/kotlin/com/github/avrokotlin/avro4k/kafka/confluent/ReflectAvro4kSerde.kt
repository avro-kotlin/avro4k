package com.github.avrokotlin.avro4k.kafka.confluent

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroDecoder
import com.github.avrokotlin.avro4k.AvroEncoder
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.serializer.AvroSerializer
import com.github.avrokotlin.avro4k.serializer.SchemaSupplierContext
import io.confluent.kafka.schemaregistry.utils.BoundedConcurrentHashMap
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.builtins.ByteArraySerializer
import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.builtins.MapSerializer
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.serializer
import org.apache.avro.Schema
import org.apache.avro.specific.SpecificData
import org.apache.kafka.common.serialization.Serdes.WrapperSerde


/**
 * A Serde that can serialize and deserialize any type supported by Avro4k, based on the confluent's schema registry.
 *
 * Enums and records are deserialized to their concrete class, by using the schema's full name as the class name.
 * It also requires to have the enums and records serializers statically accessible (their type is annotated with @Serializable), or
 * contextually accessible through the avro's serializers module.
 *
 * If it is needed to configure the [Avro] instance but is restricted to an empty constructor (like as a default serde through properties in kafka streams which is instantiated through reflection),
 * you can extend this class and provide your own [Avro] instance, or handle the configuration by overriding the [configure] method.
 *
 * To have a specific type to be serialized and deserialized known at compile time, use [Avro4kSerde].
 * To serialize and deserialize any type but without using the concrete class for enums and records, use [GenericAvro4kKafkaSerde].
 */
@ExperimentalAvro4kApi
public open class ReflectAvro4kKafkaSerde(avro: Avro = Avro) : WrapperSerde<Any>(ReflectAvro4kKafkaSerializer(avro), ReflectAvro4kKafkaDeserializer(avro))

@ExperimentalAvro4kApi
public class ReflectAvro4kKafkaSerializer(avro: Avro = Avro) : AbstractAvro4kSerializer<Any>(avro) {
    override fun getSchema(data: Any): Schema {
        return avro.schema(getSerializer(data).descriptor)
    }

    override fun getSerializer(data: Any): SerializationStrategy<Any> {
        return avro.serializersModule.serializer(data::class.java)
    }
}

@ExperimentalAvro4kApi
public class ReflectAvro4kKafkaDeserializer(avro: Avro = Avro) : AbstractAvro4kDeserializer<Any>(avro) {
    private val kSerializerCache = BoundedConcurrentHashMap<Schema, KSerializer<out Any>>()
    private val logicalTypeNameToSerializer: Map<String, KSerializer<out Any>> = avro.serializersModule.collectLogicalTypesMapping()

    override fun getDeserializer(writerSchema: Schema): DeserializationStrategy<Any> {
        return getKSerializer(writerSchema)
    }

    private fun getKSerializer(writerSchema: Schema): KSerializer<out Any> {
        return kSerializerCache.computeIfAbsent(writerSchema) {
            resolveKSerializer(writerSchema)
        }
    }

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    private fun resolveKSerializer(writerSchema: Schema): KSerializer<out Any> =
        // Let's try to use the class name from the schema
        writerSchema.getProp(SpecificData.CLASS_PROP)?.let { runCatching { serializerFromClassName(it) }.getOrNull() }
        // Then let's try to use the logical type's serializer if any
            ?: writerSchema.logicalType?.name?.let { logicalTypeNameToSerializer[it] }
            // Else infer the serializer from the schema type
            ?: when (writerSchema.type) {
                Schema.Type.NULL -> throw UnsupportedOperationException("$writerSchema schema is not supported, as writing nulls doesn't require a schema")
                Schema.Type.BOOLEAN -> Boolean.serializer()
                Schema.Type.INT -> Int.serializer()
                Schema.Type.LONG -> Long.serializer()
                Schema.Type.FLOAT -> Float.serializer()
                Schema.Type.DOUBLE -> Double.serializer()
                Schema.Type.STRING -> String.serializer()
                Schema.Type.FIXED,
                Schema.Type.BYTES -> ByteArraySerializer()

                Schema.Type.ARRAY -> ListSerializer(getKSerializer(writerSchema.elementType))
                Schema.Type.MAP -> MapSerializer(String.serializer(), getKSerializer(writerSchema.valueType))

                Schema.Type.RECORD,
                Schema.Type.ENUM -> serializerFromClassName(writerSchema.fullName)

                Schema.Type.UNION -> {
                    if (writerSchema.types.size == 2) {
                        val nullType = writerSchema.types.indexOfFirst { it.type == Schema.Type.NULL }
                        when (nullType) {
                            0 -> getKSerializer(writerSchema.types[1])
                            1 -> getKSerializer(writerSchema.types[0])
                            else -> ReflectUnionDeserializer()
                        }
                    } else {
                        ReflectUnionDeserializer()
                    }
                }
            }

    private fun serializerFromClassName(it: String) = avro.serializersModule.serializer(Class.forName(it))

    private inner class ReflectUnionDeserializer : AvroSerializer<Any>(ReflectUnionDeserializer::class.qualifiedName!!) {
        @OptIn(ExperimentalSerializationApi::class)
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
}
