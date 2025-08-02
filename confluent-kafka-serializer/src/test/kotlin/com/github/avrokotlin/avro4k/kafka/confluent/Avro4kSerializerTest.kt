package com.github.avrokotlin.avro4k.kafka.confluent

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.internal.AvroInternalExtensions
import com.github.avrokotlin.avro4k.internal.AvroInternalExtensions.encodeWithApacheEncoder
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.USE_SCHEMA_ID
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkStatic
import kotlinx.serialization.KSerializer
import kotlinx.serialization.descriptors.SerialDescriptor
import org.apache.avro.Schema
import org.apache.avro.io.Encoder
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.nio.ByteBuffer
import java.util.*

class Avro4kSerializerTest {
    private val schemaId = 42
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
    fun `serializing null returns null`(isKey: Boolean) {
        val serializer = createConfiguredSerializer(isKey = isKey)

        val actualBytes = serializer.serialize(topicName, null)

        actualBytes shouldBe null
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `serializing without pre-registered schema and with auto-registering is encoded with the registered schema id`(isKey: Boolean) {
        val serializer = createConfiguredSerializer(isKey = isKey)

        val actualBytes = serializer.serialize(topicName, value)

        val schemaId = schemaRegistryClient().getId("$topicName-${getKeyOrValue(isKey)}", AvroSchema(schema))
        // magic byte + 4 bytes id + zig-zag int
        actualBytes shouldBe magicByteBytes + getSchemaIdBytes(schemaId) + encodedBytes
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `serializing with pre-registered schema and without auto-registering is encoded with the already-registered matching schema id`(isKey: Boolean) {
        schemaRegistryClient().register("$topicName-${getKeyOrValue(isKey)}", AvroSchema(schema), 1, schemaId)
        schemaRegistryClient().register("$topicName-${getKeyOrValue(isKey)}", AvroSchema(Schema.create(Schema.Type.STRING)), 2, schemaId + 1)
        val serializer = createConfiguredSerializer(isKey = isKey, AUTO_REGISTER_SCHEMAS to false)

        val actualBytes = serializer.serialize(topicName, value)

        // magic byte + 4 bytes id + zig-zag int
        actualBytes shouldBe magicByteBytes + getSchemaIdBytes(schemaId) + encodedBytes
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `serializing with pre-registered schema and without auto-registering is encoded with the forced schema id`(isKey: Boolean) {
        schemaRegistryClient().register("$topicName-${getKeyOrValue(isKey)}", AvroSchema(schema), 1, schemaId)
        schemaRegistryClient().register("$topicName-${getKeyOrValue(isKey)}", AvroSchema(Schema.create(Schema.Type.STRING)), 2, schemaId + 1)
        val serializer = createConfiguredSerializer(isKey = true, AUTO_REGISTER_SCHEMAS to false, USE_SCHEMA_ID to schemaId)

        val actualBytes = serializer.serialize(topicName, value)

        // magic byte + 4 bytes id + zig-zag int
        actualBytes shouldBe magicByteBytes + getSchemaIdBytes(schemaId) + encodedBytes
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `serializing with pre-registered schema and without auto-registering is encoded with the latest schema id`(isKey: Boolean) {
        schemaRegistryClient().register("$topicName-${getKeyOrValue(isKey)}", AvroSchema(schema), 1, schemaId)
        schemaRegistryClient().register("$topicName-${getKeyOrValue(isKey)}", AvroSchema(Schema.create(Schema.Type.LONG)), 2, schemaId + 1)
        val serializer = createConfiguredSerializer(isKey = isKey, AUTO_REGISTER_SCHEMAS to false, USE_LATEST_VERSION to true)

        val actualBytes = serializer.serialize(topicName, value)

        // magic byte + 4 bytes id + zig-zag int
        actualBytes shouldBe magicByteBytes + getSchemaIdBytes(schemaId + 1) + encodedBytes
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `serializing with pre-registered schema and with auto-registering is encoded with the already-registered schema id`(isKey: Boolean) {
        schemaRegistryClient().register("$topicName-${getKeyOrValue(isKey)}", AvroSchema(schema), 1, schemaId)
        val serializer = createConfiguredSerializer(isKey = isKey)

        val actualBytes = serializer.serialize(topicName, value)

        // magic byte + 4 bytes id + zig-zag int
        actualBytes shouldBe magicByteBytes + getSchemaIdBytes(schemaId) + encodedBytes
    }

    private fun getKeyOrValue(isKey: Boolean) = if (isKey) "key" else "value"

    private fun getSchemaIdBytes(id: Int): ByteArray {
        val buffer = ByteBuffer.allocate(4)
        buffer.putInt(id)
        return buffer.array()
    }

    private fun createConfiguredSerializer(isKey: Boolean, vararg configs: Pair<String, Any>): Avro4kKafkaSerializer<Any> {
        mockkStatic(AvroInternalExtensions::class)
        val avro = mockk<Avro>()
        val kSerializer = mockk<KSerializer<Any>>()

        mockEncoding(kSerializer, avro, value)

        val serializer = Avro4kKafkaSerializer(kSerializer, avro)
        serializer.configure(serializerConfigs(*configs), isKey = isKey)

        return serializer
    }

    private fun mockEncoding(
        kSerializer: KSerializer<Any>,
        avro: Avro,
        value: Any
    ) {
        val serialDescriptor = mockk<SerialDescriptor>()
        every { kSerializer.descriptor } returns serialDescriptor
        every { avro.schema(serialDescriptor) } returns schema
        every { avro.encodeWithApacheEncoder(any(), kSerializer, value, any()) } answers {
            val binaryEncoder = arg<Encoder>(4)
            binaryEncoder.writeFixed(encodedBytes)
        }
    }

    private fun serializerConfigs(vararg configs: Pair<String, Any>) = mapOf<String, Any>(
        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to "mock://$registryRandomId",
    ) + configs

    private fun schemaRegistryClient(): SchemaRegistryClient =
        SchemaRegistryClientFactory.newClient(listOf("mock://$registryRandomId"), 100, listOf(AvroSchemaProvider()), emptyMap<String, Any>(), emptyMap())
}