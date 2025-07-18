package com.github.avrokotlin.avro4k.kafka.confluent

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.internal.AvroInternalExtensions
import com.github.avrokotlin.avro4k.internal.AvroInternalExtensions.decodeWithApacheDecoder
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import io.mockk.MockKStubScope
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkStatic
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import org.apache.avro.Schema
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.nio.ByteBuffer
import java.util.*
import org.apache.kafka.common.errors.SerializationException as KafkaSerializationException

class Avro4kDeserializerTest {
    private val magicByteBytes = byteArrayOf(0)
    private val schema = Schema.create(Schema.Type.INT)
    private val encodedBytes = byteArrayOf(43, 12, -56)
    private val topicName = "my-topic"
    private val value = Any()

    private lateinit var registryRandomId: UUID

    @BeforeEach
    fun setUp() {
        registryRandomId = UUID.randomUUID()
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `deserializing null returns null`(isKey: Boolean) {
        val deserializer = createConfiguredDeserializer(isKey = isKey)

        val deserializedValue = deserializer.deserialize(topicName, null)

        deserializedValue shouldBe null
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `deserializing empty byte array fails`(isKey: Boolean) {
        val deserializer = createConfiguredDeserializer(isKey = isKey)

        shouldThrow<KafkaSerializationException> {
            deserializer.deserialize(topicName, byteArrayOf())
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `deserializing wrong magic byte fails`(isKey: Boolean) {
        val deserializer = createConfiguredDeserializer(isKey = isKey)

        shouldThrow<KafkaSerializationException> {
            // Expect 0
            deserializer.deserialize(topicName, byteArrayOf(1))
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `deserializing truncated byte array fails (expect 5 bytes minimum)`(isKey: Boolean) {
        val deserializer = createConfiguredDeserializer(isKey = isKey)

        shouldThrow<KafkaSerializationException> {
            deserializer.deserialize(topicName, byteArrayOf(0))
        }
        shouldThrow<KafkaSerializationException> {
            deserializer.deserialize(topicName, byteArrayOf(0, 1))
        }
        shouldThrow<KafkaSerializationException> {
            deserializer.deserialize(topicName, byteArrayOf(0, 1, 2))
        }
        shouldThrow<KafkaSerializationException> {
            deserializer.deserialize(topicName, byteArrayOf(0, 1, 2, 3))
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `nominal case - deserializing well formatted bytes with already registered schema returns the expected value`(isKey: Boolean) {
        schemaRegistryClient().register("$topicName-${getKeyOrValue(isKey)}", AvroSchema(schema), 1, 42)
        val deserializer = createConfiguredDeserializer(isKey = isKey)

        val deserializedValue = deserializer.deserialize(topicName, magicByteBytes + getSchemaIdBytes(42) + encodedBytes)

        deserializedValue shouldBe value
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `any avro SerializationException is wrapped to kafka exception`(isKey: Boolean) {
        schemaRegistryClient().register("$topicName-${getKeyOrValue(isKey)}", AvroSchema(schema), 1, 42)

        mockkStatic(AvroInternalExtensions::class)
        val avro = mockk<Avro>()
        val kSerializer = mockk<KSerializer<Any>>()

        everyDecoding(kSerializer, avro) throws SerializationException("boom")

        val deserializer = Avro4kKafkaDeserializer(kSerializer, avro)
        deserializer.configure(deserializerConfigs(), isKey = isKey)

        shouldThrow<KafkaSerializationException> {
            deserializer.deserialize(topicName, magicByteBytes + getSchemaIdBytes(42) + encodedBytes)
        }
    }

    private fun getKeyOrValue(isKey: Boolean) = if (isKey) "key" else "value"

    private fun getSchemaIdBytes(id: Int): ByteArray {
        val buffer = ByteBuffer.allocate(4)
        buffer.putInt(id)
        return buffer.array()
    }

    private fun createConfiguredDeserializer(isKey: Boolean, vararg configs: Pair<String, Any>): Avro4kKafkaDeserializer<Any> {
        mockkStatic(AvroInternalExtensions::class)
        val avro = mockk<Avro>()
        val kSerializer = mockk<KSerializer<Any>>()

        everyDecoding(kSerializer, avro) returns value

        val deserializer = Avro4kKafkaDeserializer(kSerializer, avro)
        deserializer.configure(deserializerConfigs(*configs), isKey = isKey)

        return deserializer
    }

    private fun everyDecoding(
        kSerializer: KSerializer<Any>,
        avro: Avro
    ): MockKStubScope<Any, Any> {
        val serialDescriptor = mockk<SerialDescriptor>()
        every { kSerializer.descriptor } returns serialDescriptor
        every { avro.schema(serialDescriptor) } returns schema
        return every { avro.decodeWithApacheDecoder(schema, kSerializer, any()) }
    }

    private fun deserializerConfigs(vararg configs: Pair<String, Any>) = mapOf<String, Any>(
        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to "mock://$registryRandomId",
    ) + configs

    private fun schemaRegistryClient(): SchemaRegistryClient =
        SchemaRegistryClientFactory.newClient(listOf("mock://$registryRandomId"), 100, listOf(AvroSchemaProvider()), emptyMap<String, Any>(), emptyMap())
}