package com.github.avrokotlin.avro4k.kafka.confluent

import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import org.apache.avro.Schema
import org.apache.kafka.common.serialization.Deserializer
import java.util.*
import kotlin.math.absoluteValue

object KafkaSerializationTestHelpers {
    const val TOPIC_NAME = "test-record"

    fun configuredConfluentDeserializer(testId: UUID, isKey: Boolean): Deserializer<Any?> {
        return KafkaAvroDeserializer().apply { configure(getConfigs(testId), isKey) }
    }

    fun registerSchema(testId: UUID, schema: Schema, isKey: Boolean): Int {
        schemaRegistryClient(testId).register("$TOPIC_NAME-${getKeyOrValue(isKey)}", AvroSchema(schema), -1 /*useless value for tests*/, makeSchemaId(testId))
        return makeSchemaId(testId)
    }

    fun getConfigs(testId: UUID, useSchemaIdFromTestId: Boolean = false, autoRegisterSchema: Boolean = false): Map<String, Any> {
        return mapOf<String, Any>(
            AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to "mock://$testId",
            AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS to autoRegisterSchema,
            AbstractKafkaSchemaSerDeConfig.USE_SCHEMA_ID to (if (useSchemaIdFromTestId) makeSchemaId(testId) else -1),
            KafkaAvroDeserializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG to true,
        )
    }

     fun makeSchemaId(testId: UUID) = testId.hashCode().absoluteValue

     fun getKeyOrValue(isKey: Boolean) = if (isKey) "key" else "value"

     fun schemaRegistryClient(testId: UUID): SchemaRegistryClient =
        SchemaRegistryClientFactory.newClient(listOf("mock://$testId"), 100, listOf(AvroSchemaProvider()), emptyMap<String, Any>(), emptyMap())

    fun Any?.printWithType(): String {
        if (this == null) return "<null>"
        return "${this::class.simpleName}<$this>"
    }
}
