package com.github.avrokotlin.avro4k.kafka.confluent

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.InternalAvro4kApi
import com.github.avrokotlin.avro4k.decodeFromSource
import com.github.avrokotlin.avro4k.encodeToSink
import com.github.avrokotlin.avro4k.internal.nullable
import io.confluent.kafka.schemaregistry.ParsedSchema
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import kotlinx.io.Buffer
import kotlinx.io.UnsafeIoApi
import kotlinx.io.readByteArray
import kotlinx.io.unsafe.UnsafeBufferOperations
import kotlinx.io.write
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.SerializationException
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.serializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericContainer
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer
import java.io.IOException
import java.io.InterruptedIOException
import java.nio.ByteBuffer
import org.apache.kafka.common.errors.SerializationException as KafkaSerializationException


@ExperimentalAvro4kApi
public class KafkaAvro4kSerializerConfig(configs: Map<String, *>) : AbstractKafkaSchemaSerDeConfig(baseConfigDef(), configs)

@ExperimentalAvro4kApi
public class KafkaAvro4kDeserializerConfig(configs: Map<String, *>) : AbstractKafkaSchemaSerDeConfig(baseConfigDef(), configs)

@ExperimentalAvro4kApi
public abstract class AbstractAvro4kKafkaSerializer<T : Any>(
    protected val avro: Avro,
) : Serializer<T>, AbstractKafkaSchemaSerDe() {
    private lateinit var writerSchemaProvider: (topic: String, data: T) -> IdentifiedSchema

    protected abstract val serializer: SerializationStrategy<T>

    /**
     * Provides the Avro schema for the given value.
     *
     * This schema is the one going to be used as the writer schema when serializing the value.
     */
    protected open fun getSchema(value: T): Schema {
        return getAnySchema(value)
    }

    @OptIn(InternalAvro4kApi::class)
    private fun getAnySchema(value: Any): Schema {
        if (value is GenericContainer) {
            return value.schema
        }
        if (value is ByteBuffer) {
            return Schema.create(Schema.Type.BYTES)
        }
        if (value is Utf8) {
            return Schema.create(Schema.Type.STRING)
        }
        if (value is Collection<*>) {
            if (value.isEmpty()) {
                throw SerializationException("Cannot determine schema for an empty collection. Please provide a non-empty collection or specify the schema using ${GenericData.Array::class.qualifiedName}.")
            }
            var element = value.first()
            var isElementNullable = false
            if (element == null) {
                isElementNullable = true
                element = value.asSequence().firstOrNull { it!= null }
                if (element == null)
                    throw SerializationException("Cannot determine schema for a collection with only null elements. Please provide at least one non-null element or specify the schema using ${GenericData.Array::class.qualifiedName}.")
            }
            return Schema.createArray(getAnySchema(element).let { if (isElementNullable) it.nullable else it })
        }
        if (value is Map<*, *>) {
            if (value.isEmpty()) {
                throw SerializationException("Cannot determine schema for an empty map. Please provide a non-empty map or specify the schema using ${GenericMap::class.qualifiedName}.")
            }
            var element = value.asSequence().first().value
            var isElementNullable = false
            if (element == null) {
                isElementNullable = true
                element = value.asSequence().firstNotNullOfOrNull { it.value }
                if (element == null)
                    throw SerializationException("Cannot determine schema for a map with only null values. Please provide at least one non-null value or specify the schema using ${GenericMap::class.qualifiedName}.")
            }
            return Schema.createMap(getAnySchema(element).let { if (isElementNullable) it.nullable else it })
        }
        // All the rest should be serializable to have its schema inferred
        val serializer = avro.serializersModule.serializer(value::class.java)
        return avro.schema(serializer.descriptor)
    }

    override fun serialize(topic: String, data: T?): ByteArray? {
        if (data == null) return null

        try {
            val (id, writerSchema) = writerSchemaProvider(topic, data)
            val buffer = Buffer()
            buffer.writeByte(MAGIC_BYTE)
            buffer.writeInt(id)
            if (writerSchema.type == Schema.Type.BYTES) {
                when (data) {
                    is ByteArray -> buffer.write(data)
                    is ByteBuffer -> buffer.write(data)
                    else -> throw KafkaSerializationException("Unsupported type ${data::class.java.name} for root BYTES schema")
                }
            } else {
                avro.encodeToSink(writerSchema, serializer, data, buffer)
            }
            return buffer.readByteArray()
        } catch (e: KafkaSerializationException) {
            throw e
        } catch (e: Exception) {
            throw KafkaSerializationException("Error serializing Avro message", e)
        }
    }

    override fun configure(configs: Map<String, *>, isKey: Boolean) {
        this.isKey = isKey

        val config = KafkaAvro4kSerializerConfig(configs)
        configureClientProperties(config, AvroSchemaProvider())
        val normalizeSchema = config.normalizeSchema()

        if (config.autoRegisterSchema()) {
            require(config.useSchemaId() < 0) { "Cannot set both auto-register and use-schema-id" }
            require(!config.useLatestVersion()) { "Cannot set both auto-register and use-latest-version" }
            writerSchemaProvider = { topic, data -> autoRegisterSchema(topic, data, normalizeSchema) }
        } else if (config.useSchemaId() >= 0) {
            require(!config.useLatestVersion()) { "Cannot set both auto-register and use-latest-version" }
            val schemaId = config.useSchemaId()
            writerSchemaProvider = { topic, data -> useSpecificSchema(topic, data, schemaId) }
        } else if (config.useLatestVersion()) {
            writerSchemaProvider = ::useLatestVersion
        } else {
            writerSchemaProvider = { topic, data -> getMatchingSchemaForValue(topic, data, normalizeSchema) }
        }
    }

    private fun autoRegisterSchema(topic: String, value: T, normalizeSchema: Boolean): IdentifiedSchema {
        val schema = AvroSchema(getSchema(value))
        val subject = getSubjectName(topic, value, schema)

        val schemaId = runMappingException("Error while auto-registering for subject $subject the schema $schema") {
            schemaRegistry.register(subject, schema, normalizeSchema)
        }
        return IdentifiedSchema(schemaId, schema.rawSchema())
    }

    private fun useSpecificSchema(topic: String, value: T, schemaId: Int): IdentifiedSchema {
        val subject = getSubjectName(topic, value)

        val schema = runMappingException("Error while retrieving the schema id $schemaId for subject $subject") {
            schemaRegistry.getSchemaBySubjectAndId(subject, schemaId)
        }
        return IdentifiedSchema(schemaId, schema.rawSchema() as Schema)
    }

    private fun useLatestVersion(topic: String, value: T): IdentifiedSchema {
        val subject = getSubjectName(topic, value)

        val metadata = runMappingException("Error while retrieving the latest schema for subject $subject") {
            schemaRegistry.getLatestSchemaMetadata(subject)
        }
        val parsedSchema = schemaRegistry.parseSchema(io.confluent.kafka.schemaregistry.client.rest.entities.Schema(subject, metadata))
            .orElseThrow { KafkaSerializationException("Error parsing Avro schema from latest schema $metadata") }
        return IdentifiedSchema(metadata.id, parsedSchema.rawSchema() as Schema)
    }

    private fun getMatchingSchemaForValue(topic: String, value: T, normalizeSchema: Boolean): IdentifiedSchema {
        val schema = AvroSchema(getSchema(value))
        val subject = getSubjectName(topic, value, schema)

        val schemaId = runMappingException("Error while retrieving the schema id for subject $subject and schema $schema") {
            schemaRegistry.getId(subject, schema, normalizeSchema)
        }
        return IdentifiedSchema(schemaId, schema.rawSchema())
    }

    private data class IdentifiedSchema(
        val id: Int,
        val schema: Schema
    )

    private fun <T> runMappingException(errorMessage: String, block: () -> T): T {
        try {
            return block()
        } catch (e: InterruptedIOException) {
            throw TimeoutException(errorMessage, e)
        } catch (e: RestClientException) {
            throw toKafkaException(e, errorMessage)
        } catch (e: Exception) {
            throw KafkaSerializationException(errorMessage, e)
        }
    }

    private fun getSubjectName(topic: String, data: T, schema: AvroSchema? = null): String? =
        getSubjectName(topic, isKey, data, if (strategyUsesSchema(isKey)) schema ?: AvroSchema(getSchema(data)) else null)

    override fun close() {
        super<AbstractKafkaSchemaSerDe>.close()
    }
}

@ExperimentalAvro4kApi
public abstract class AbstractAvro4kKafkaDeserializer<T : Any>(
    protected val avro: Avro,
) : Deserializer<T>, AbstractKafkaSchemaSerDe() {
    override fun configure(configs: Map<String, *>, isKey: Boolean) {
        this.isKey = isKey

        val config = KafkaAvro4kDeserializerConfig(configs)
        configureClientProperties(config, AvroSchemaProvider())
    }

    override fun deserialize(topic: String, data: ByteArray?): T? {
        if (data == null) return null

        @OptIn(UnsafeIoApi::class)
        val buffer = Buffer().apply { UnsafeBufferOperations.moveToTail(this, data) }
        val writerSchema = try {
            val magic = buffer.readByte()
            if (magic != MAGIC_BYTE) {
                throw KafkaSerializationException("Unknown magic byte: $magic")
            }
            val writerSchema = retrieveSchema(topic, buffer.readInt())
            if (writerSchema.type == Schema.Type.BYTES) {
                // Bytes in the confluent serializer are not as usual bytes which are prefixed with a size
                // They are just the raw bytes to be extracted starting just after the schema id
                @Suppress("UNCHECKED_CAST")
                return buffer.readByteArray() as T
            }
            writerSchema
        } catch (e: KafkaException) {
            throw e
        } catch (e: Exception) {
            throw KafkaSerializationException("Error deserializing Avro message's schema identifier", e)
        }
        try {
            return avro.decodeFromSource(writerSchema, deserializer, buffer)
        } catch (e: Exception) {
            throw KafkaSerializationException("Error deserializing Avro message to type ${deserializer.descriptor} from writer-schema $writerSchema", e)
        }
    }

    protected abstract val deserializer: DeserializationStrategy<T>

    private fun retrieveSchema(topic: String, schemaId: Int): Schema {
        try {
            val subjectName: String = getSubjectName(topic, isKey, null, null)
            return schemaRegistry.getSchemaBySubjectAndId(subjectName, schemaId).avroSchema()
        } catch (e: InterruptedIOException) {
            throw TimeoutException(errorRetrievingSchemaMessage(schemaId), e)
        } catch (e: IOException) {
            throw KafkaSerializationException(errorRetrievingSchemaMessage(schemaId), e)
        } catch (e: RestClientException) {
            throw toKafkaException(e, errorRetrievingSchemaMessage(schemaId))
        }
    }

    private fun errorRetrievingSchemaMessage(schemaId: Int) =
        "Error retrieving Avro ${if (isKey) "key" else "value"} schema for id $schemaId"

    override fun close() {
        super<AbstractKafkaSchemaSerDe>.close()
    }
}

private fun ParsedSchema.avroSchema(): Schema = rawSchema() as Schema