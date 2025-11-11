package com.github.avrokotlin.avro4k.kafka.confluent

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.InternalAvro4kApi
import com.github.avrokotlin.avro4k.internal.decodeWithApacheDecoder
import com.github.avrokotlin.avro4k.internal.encodeWithApacheEncoder
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer
import io.confluent.kafka.serializers.AbstractKafkaAvroSerializer
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.serializers.NonRecordContainer
import io.confluent.kafka.serializers.schema.id.SchemaId
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.SerializationException
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.serializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericContainer
import org.apache.avro.generic.GenericData
import org.apache.avro.io.DatumReader
import org.apache.avro.io.DatumWriter
import org.apache.avro.io.Decoder
import org.apache.avro.io.Encoder
import org.apache.avro.io.EncoderFactory
import org.apache.avro.util.Utf8
import org.apache.kafka.common.errors.InvalidConfigurationException
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.InterruptedIOException
import java.nio.ByteBuffer
import java.util.concurrent.ExecutionException
import org.apache.kafka.common.errors.SerializationException as KafkaSerializationException

@ExperimentalAvro4kApi
public abstract class AbstractAvro4kKafkaSerializer<T : Any>(
    protected val avro: Avro,
) : Serializer<T>, AbstractKafkaAvroSerializer() {
    @InternalAvro4kApi
    protected abstract val serializer: SerializationStrategy<T>

    override fun configure(configs: Map<String, Any?>, isKey: Boolean) {
        this.isKey = isKey
        super<AbstractKafkaAvroSerializer>.configure(serializerConfig(configs))
    }

    @InternalAvro4kApi
    protected fun initialize(
        schemaRegistry: SchemaRegistryClient?,
        props: Map<String, Any?>,
        isKey: Boolean,
    ) {
        super.schemaRegistry = schemaRegistry
        super.ticker = ticker(schemaRegistry)

        val props =
            if (schemaRegistry != null && AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG !in props) {
                // ensure the schema registry url config is set to some value, otherwise it raises an exception
                props + mapOf(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to "")
            } else {
                props
            }
        configure(props, isKey)
    }

    /**
     * Provides the Avro schema for the given value.
     *
     * This schema is the one going to be used as the writer schema when serializing the value.
     */
    @InternalAvro4kApi
    protected open fun getSchema(value: T): Schema {
        return getAnySchema(value)
    }

    private fun getAnySchema(value: Any): Schema {
        when (value) {
            is GenericContainer -> return value.schema
            is ByteBuffer -> return Schema.create(Schema.Type.BYTES)
            is Utf8 -> return Schema.create(Schema.Type.STRING)
            is Collection<*> -> return Schema.createArray(inferItemSchema(value, "${GenericData.Array::class.qualifiedName} or ${NonRecordContainer::class.qualifiedName}"))
            is Map<*, *> -> return Schema.createMap(inferItemSchema(value.values, NonRecordContainer::class.qualifiedName!!))

            // All the rest should be serializable to have its schema inferred
            else -> {
                val serializer = avro.serializersModule.serializer(value::class.java)
                return avro.schema(serializer.descriptor)
            }
        }
    }

    private fun inferItemSchema(value: Collection<*>, typeToUse: String): Schema {
        if (value.isEmpty()) {
            throw SerializationException("Cannot determine schema for an empty collection. Please provide a non-empty collection or specify the schema using $typeToUse.")
        }
        val element =
            value.first()
                ?: throw SerializationException(
                    "Cannot determine schema for a collection with only null elements. Please provide at least one non-null element or specify the schema using $typeToUse."
                )
        return getAnySchema(element)
    }

    override fun serialize(topic: String?, data: T?): ByteArray? {
        return serialize(topic, null, data)
    }

    override fun serialize(topic: String?, headers: Headers?, data: T?): ByteArray? {
        if (data == null) {
            return null
        }
        val schema = getSchema(data)
        val data =
            if (schema.type == Schema.Type.BYTES) {
                // confluent serializer expects root BYTES to be ByteArray or ByteBuffer, so it is not compatible with value classes wrapping ByteArray or ByteBuffer.
                // We serialize the data to bytes ourselves here.
                unwrapRootByteArray(schema, data)
            } else {
                data
            }
        val avroSchema = AvroSchema(schema)
        return serializeImpl(getSubjectName(topic, isKey, data, avroSchema), topic, headers, data, avroSchema)
    }

    private fun unwrapRootByteArray(schema: Schema, data: T): ByteArray {
        var bytes: ByteArray? = null
        avro.encodeWithApacheEncoder(
            schema,
            serializer,
            data,
            object : NotImplementedEncoder() {
                override fun writeBytes(b: ByteBuffer) {
                    bytes = b.array()
                }

                override fun writeBytes(b: ByteArray, start: Int, len: Int) {
                    bytes = b.copyOfRange(start, start + len)
                }
            }
        )
        return bytes ?: throw UnsupportedOperationException("avro.encodeWithApacheEncoder did not call writeBytes")
    }

    // TODO this will override super method when a new version of the SR is released
    @InternalAvro4kApi
    protected open fun getDatumWriter(value: Any, rawSchema: Schema): DatumWriter<Any?> {
        return object : DatumWriter<Any?> {
            override fun write(value: Any?, out: Encoder) {
                @Suppress("UNCHECKED_CAST")
                avro.encodeWithApacheEncoder(rawSchema, serializer, value as T, out)
            }

            override fun setSchema(schema: Schema) {
                throw UnsupportedOperationException()
            }
        }
    }

    override fun close() {
        super<AbstractKafkaAvroSerializer>.close()
    }

    // <editor-fold desc="TODO: remove this copy of private methods from AbstractKafkaAvroSerializer when we can override getDatumWriter">
    override fun serializeImpl(subject: String, topic: String?, headers: Headers?, value: Any?, schema: AvroSchema): ByteArray? {
        if (schemaRegistry == null) {
            val userFriendlyMsgBuilder = StringBuilder()
            userFriendlyMsgBuilder.append("You must configure() before serialize()")
            userFriendlyMsgBuilder.append(" or use serializer constructor with SchemaRegistryClient")
            throw InvalidConfigurationException(userFriendlyMsgBuilder.toString())
        }
        if (value == null) {
            return null
        }
        var restClientErrorMsg = ""
        var value = value
        var schema = schema
        try {
            var schemaId: SchemaId
            if (autoRegisterSchema) {
                restClientErrorMsg = "Error registering Avro schema"
                val s =
                    registerWithResponse(subject, schema, normalizeSchema, propagateSchemaTags)
                if (s.schema != null) {
                    val optSchema = schemaRegistry.parseSchema(s)
                    if (optSchema.isPresent) {
                        schema = optSchema.get() as AvroSchema
                        schema = schema.copy(s.version)
                    }
                }
                schemaId = SchemaId(AvroSchema.TYPE, s.id, s.guid)
            } else if (useSchemaId >= 0) {
                restClientErrorMsg = "Error retrieving schema ID"
                schema = (lookupSchemaBySubjectAndId(subject, useSchemaId, schema, idCompatStrict) as AvroSchema?)!!
                val schemaEntity = io.confluent.kafka.schemaregistry.client.rest.entities.Schema(subject, null, useSchemaId, schema)
                schemaId = SchemaId(AvroSchema.TYPE, useSchemaId, schemaEntity.guid)
            } else if (metadata != null) {
                restClientErrorMsg = "Error retrieving latest with metadata '$metadata'"
                val extendedSchema = getLatestWithMetadata(subject)
                schema = (extendedSchema.getSchema() as AvroSchema?)!!
                schemaId =
                    SchemaId(
                        AvroSchema.TYPE,
                        extendedSchema.getId(),
                        extendedSchema.getGuid()
                    )
            } else if (useLatestVersion) {
                restClientErrorMsg = "Error retrieving latest version of Avro schema"
                val extendedSchema = lookupLatestVersion(subject, schema, latestCompatStrict)
                schema = (extendedSchema.getSchema() as AvroSchema?)!!
                schemaId =
                    SchemaId(
                        AvroSchema.TYPE,
                        extendedSchema.getId(),
                        extendedSchema.getGuid()
                    )
            } else {
                restClientErrorMsg = "Error retrieving Avro schema"
                val response =
                    schemaRegistry.getIdWithResponse(subject, schema, normalizeSchema)
                schemaId = SchemaId(AvroSchema.TYPE, response.id, response.guid)
            }
            AvroSchemaUtils.setThreadLocalData(
                schema.rawSchema(),
                avroUseLogicalTypeConverters,
                avroReflectionAllowNull
            )
            try {
                value = executeRules(subject, topic, headers, RuleMode.WRITE, null, schema, value)
            } finally {
                AvroSchemaUtils.clearThreadLocalData()
            }

            schemaIdSerializer(isKey).use { schemaIdSerializer ->
                ByteArrayOutputStream().use { baos ->
                    val value = if (value is NonRecordContainer) value.value else value
                    val rawSchema = schema.rawSchema()
                    if (rawSchema.type == Schema.Type.BYTES) {
                        when (value) {
                            is ByteArray -> baos.write(value)
                            is ByteBuffer -> baos.write(value.array())
                            else -> throw KafkaSerializationException("Unrecognized bytes object of type: " + value.javaClass.getName())
                        }
                    } else {
                        writeDatum(baos, value, rawSchema)
                    }
                    val payload = baos.toByteArray()
                    return schemaIdSerializer.serialize(topic, isKey, headers, payload, schemaId)
                }
            }
        } catch (ex: ExecutionException) {
            throw KafkaSerializationException("Error serializing Avro message", ex.cause)
        } catch (e: InterruptedIOException) {
            throw TimeoutException("Error serializing Avro message", e)
        } catch (e: IOException) {
            throw KafkaSerializationException("Error serializing Avro message", e)
        } catch (e: RuntimeException) {
            throw KafkaSerializationException("Error serializing Avro message", e)
        } catch (e: RestClientException) {
            throw toKafkaException(e, restClientErrorMsg + schema)
        } finally {
            postOp(value)
        }
    }

    private val encoderFactory: EncoderFactory = EncoderFactory.get()

    private fun writeDatum(out: ByteArrayOutputStream, value: Any, rawSchema: Schema) {
        val encoder = encoderFactory.directBinaryEncoder(out, null)
        getDatumWriter(value, rawSchema).write(value, encoder)
        encoder.flush()
    }
    // </editor-fold>
}

@ExperimentalAvro4kApi
public abstract class AbstractAvro4kKafkaDeserializer<T : Any>(
    protected val avro: Avro,
) : Deserializer<T>, AbstractKafkaAvroDeserializer() {
    @InternalAvro4kApi
    protected fun initialize(
        schemaRegistry: SchemaRegistryClient?,
        props: Map<String, Any?>,
        isKey: Boolean,
    ) {
        super.schemaRegistry = schemaRegistry
        super.ticker = ticker(schemaRegistry)

        val props =
            if (schemaRegistry != null && AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG !in props) {
                // ensure the schema registry url config is set to some value, otherwise it raises an exception
                props + mapOf(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to "")
            } else {
                props
            }
        configure(props, isKey)
    }

    override fun configure(configs: Map<String, Any?>, isKey: Boolean) {
        this.isKey = isKey
        super<AbstractKafkaAvroDeserializer>.configure(deserializerConfig(configs))
    }

    override fun getDatumReader(writerSchema: Schema, readerSchema: Schema?): DatumReader<*> {
        // Avro4k natively supports schema evolution, and it needs to rely on the writer schema only to decode the data.
        return avro.getDatumReader<Any>(writerSchema, deserializer)
    }

    @InternalAvro4kApi
    protected abstract val deserializer: DeserializationStrategy<T>

    @InternalAvro4kApi
    protected abstract val schema: Schema?

    override fun deserialize(topic: String?, data: ByteArray?): T? {
        return deserialize(topic, null, data)
    }

    override fun deserialize(topic: String?, headers: Headers?, data: ByteArray?): T? {
        @Suppress("UNCHECKED_CAST") // getDatumReader is expected to deserialize to T
        return super<AbstractKafkaAvroDeserializer>.deserialize(topic, isKey, headers, data, schema) as T?
    }

    override fun close() {
        super<AbstractKafkaAvroDeserializer>.close()
    }
}

internal fun <T> Avro.getDatumReader(readerSchema: Schema, deserializer: DeserializationStrategy<T>): DatumReader<T> {
    return Avro4kDatumReader(this, readerSchema, deserializer)
}

internal class Avro4kDatumReader<T>(
    private val avro: Avro,
    private val readerSchema: Schema,
    private val deserializer: DeserializationStrategy<T>,
) : DatumReader<T> {
    override fun read(reuse: T?, `in`: Decoder): T {
        return avro.decodeWithApacheDecoder(readerSchema, deserializer, `in`)
    }

    override fun setSchema(schema: Schema) {
        throw UnsupportedOperationException()
    }
}

private abstract class NotImplementedEncoder : Encoder() {
    override fun writeNull(): Unit = throw UnsupportedOperationException()

    override fun writeBoolean(b: Boolean): Unit = throw UnsupportedOperationException()

    override fun writeInt(i: Int): Unit = throw UnsupportedOperationException()

    override fun writeLong(l: Long): Unit = throw UnsupportedOperationException()

    override fun writeFloat(v: Float): Unit = throw UnsupportedOperationException()

    override fun writeDouble(v: Double): Unit = throw UnsupportedOperationException()

    override fun writeString(s: Utf8): Unit = throw UnsupportedOperationException()

    override fun writeBytes(b: ByteBuffer): Unit = throw UnsupportedOperationException()

    override fun writeBytes(b: ByteArray, start: Int, len: Int): Unit = throw UnsupportedOperationException()

    override fun writeFixed(b: ByteArray, start: Int, len: Int): Unit = throw UnsupportedOperationException()

    override fun writeEnum(e: Int): Unit = throw UnsupportedOperationException()

    override fun writeArrayStart(): Unit = throw UnsupportedOperationException()

    override fun setItemCount(itemCount: Long): Unit = throw UnsupportedOperationException()

    override fun startItem(): Unit = throw UnsupportedOperationException()

    override fun writeArrayEnd(): Unit = throw UnsupportedOperationException()

    override fun writeMapStart(): Unit = throw UnsupportedOperationException()

    override fun writeMapEnd(): Unit = throw UnsupportedOperationException()

    override fun writeIndex(i: Int): Unit = throw UnsupportedOperationException()

    override fun flush(): Unit = throw UnsupportedOperationException()
}