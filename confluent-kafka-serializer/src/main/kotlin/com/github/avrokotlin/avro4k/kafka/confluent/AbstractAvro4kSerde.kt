package com.github.avrokotlin.avro4k.kafka.confluent

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.encodeToSink
import com.github.avrokotlin.avro4k.internal.AvroInternalExtensions.decodeWithApacheDecoder
import io.confluent.kafka.schemaregistry.ParsedSchema
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.SerializationStrategy
import org.apache.avro.Schema
import org.apache.avro.io.DecoderFactory
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
public abstract class AbstractAvro4kSerializer<T>(
    protected val avro: Avro,
) : Serializer<T>, AbstractKafkaSchemaSerDe() {
    private lateinit var writerSchemaRetriever: (topic: String, data: T) -> IdentifiedSchema

    protected abstract fun getSerializer(data: T): SerializationStrategy<T>
    protected abstract fun getSchema(data: T): Schema

    override fun serialize(topic: String, data: T?): ByteArray? {
        if (data == null) return null

        try {
            val (id, writerSchema) = writerSchemaRetriever(topic, data)
            val buffer = okio.Buffer()
            buffer.writeByte(MAGIC_BYTE.toInt())
            buffer.writeInt(id)
            if (writerSchema.type == Schema.Type.BYTES) {
                when (data) {
                    is ByteArray -> buffer.write(data)
                    is ByteBuffer -> buffer.write(data)
                    else -> throw KafkaSerializationException("Unsupported type ${data!!::class.java.name} for root BYTES schema")
                }
            } else {
                avro.encodeToSink(writerSchema, getSerializer(data), data, buffer)
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
            writerSchemaRetriever = { topic, data -> autoRegisterSchema(topic, data, normalizeSchema) }
        } else if (config.useSchemaId() >= 0) {
            require(!config.useLatestVersion()) { "Cannot set both auto-register and use-latest-version" }
            val schemaId = config.useSchemaId()
            writerSchemaRetriever = { topic, data -> useSpecificSchema(topic, data, schemaId) }
        } else if (config.useLatestVersion()) {
            writerSchemaRetriever = ::useLatestVersion
        } else {
            writerSchemaRetriever = { topic, data -> getMatchingSchemaForData(topic, data, normalizeSchema) }
        }
    }

    private fun autoRegisterSchema(topic: String, data: T, normalizeSchema: Boolean): IdentifiedSchema {
        val schema = AvroSchema(getSchema(data))
        val subject = getSubjectName(topic, data, schema)

        val schemaId = runMappingException("Error while auto-registering for subject $subject the schema $schema") {
            schemaRegistry.register(subject, schema, normalizeSchema)
        }
        return IdentifiedSchema(schemaId, schema.rawSchema())
    }

    private fun useSpecificSchema(topic: String, data: T, schemaId: Int): IdentifiedSchema {
        val subject = getSubjectName(topic, data)

        val schema = runMappingException("Error while retrieving the schema id $schemaId for subject $subject") {
            schemaRegistry.getSchemaBySubjectAndId(subject, schemaId)
        }
        return IdentifiedSchema(schemaId, schema.rawSchema() as Schema)
    }

    private fun useLatestVersion(topic: String, data: T): IdentifiedSchema {
        val subject = getSubjectName(topic, data)

        val metadata = runMappingException("Error while retrieving the latest schema for subject $subject") {
            schemaRegistry.getLatestSchemaMetadata(subject)
        }
        val parsedSchema = schemaRegistry.parseSchema(io.confluent.kafka.schemaregistry.client.rest.entities.Schema(subject, metadata))
            .orElseThrow { KafkaSerializationException("Error parsing Avro schema from latest schema $metadata") }
        return IdentifiedSchema(metadata.id, parsedSchema.rawSchema() as Schema)
    }

    private fun getMatchingSchemaForData(topic: String, data: T, normalizeSchema: Boolean): IdentifiedSchema {
        val schema = AvroSchema(getSchema(data))
        val subject = getSubjectName(topic, data, schema)

        val schemaId = runMappingException("Error while retrieving the schema id for subject $subject and schema $schema") {
            schemaRegistry.getId(subject, schema, normalizeSchema)
        }
        return IdentifiedSchema(schemaId, schema.rawSchema())
    }

    protected data class IdentifiedSchema(
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
public abstract class AbstractAvro4kDeserializer<T>(
    protected val avro: Avro,
) : Deserializer<T>, AbstractKafkaSchemaSerDe() {
    private companion object {
        private const val PAYLOAD_OFFSET = Byte.SIZE_BYTES + Int.SIZE_BYTES
    }

    override fun configure(configs: Map<String, *>, isKey: Boolean) {
        this.isKey = isKey

        val config = KafkaAvro4kDeserializerConfig(configs)
        configureClientProperties(config, AvroSchemaProvider())
    }

    override fun deserialize(topic: String, data: ByteArray?): T? {
        if (data == null) return null

        if (data.isEmpty()) {
            throw KafkaSerializationException("The input byte-array is empty")
        }
        if (data[0] != MAGIC_BYTE) {
            throw KafkaSerializationException("Unknown magic byte: ${data[0]}")
        }
        if (data.size < PAYLOAD_OFFSET) {
            throw KafkaSerializationException("The input byte-array doesn't contain enough bytes to read the writer-schema id")
        }
        val schemaId = ByteBuffer.wrap(data, Byte.SIZE_BYTES, idSize).getInt()
        val writerSchema = retrieveSchema(topic, schemaId)
        if (writerSchema.type == Schema.Type.BYTES) {
            // Bytes in the confluent serializer are not as usual bytes which are prefixed with a size
            // They are just the raw bytes to be extracted starting just after the schema id
            @Suppress("UNCHECKED_CAST")
            return data.copyOfRange(PAYLOAD_OFFSET, data.size) as T
        }
        val decoder = DecoderFactory.get().binaryDecoder(data, PAYLOAD_OFFSET, data.size - PAYLOAD_OFFSET, null)
        val kSerializer = getDeserializer(writerSchema)

        try {
            return avro.decodeWithApacheDecoder(writerSchema, kSerializer, decoder)
        } catch (e: KafkaException) {
            throw e
        } catch (e: Exception) {
            throw KafkaSerializationException("Error deserializing Avro message to type ${kSerializer.descriptor} from writer-schema $writerSchema", e)
        }
    }

    protected abstract fun getDeserializer(writerSchema: Schema): DeserializationStrategy<T>

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