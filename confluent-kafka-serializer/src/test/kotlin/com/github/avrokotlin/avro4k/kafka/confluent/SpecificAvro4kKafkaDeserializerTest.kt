package com.github.avrokotlin.avro4k.kafka.confluent

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.beInstanceOf
import kotlinx.serialization.Serializable

class SpecificAvro4kKafkaDeserializerTest : StringSpec() {
    init {
        "value class of String should be deserializable" {
            val schemaRegistry = MockSchemaRegistryClient()
            val bytes = GenericAvro4kKafkaSerializer(isKey = true, schemaRegistry = schemaRegistry).serialize("topic", "test data")
            val deserializer = SpecificAvro4kKafkaDeserializer(StringWrapper.serializer(), isKey = true, schemaRegistry = schemaRegistry)

            val deserialized = deserializer.deserialize("topic", bytes)

            deserialized shouldBe StringWrapper("test data")
        }

        "value class of ByteArray should be serializable" {
            val schemaRegistry = MockSchemaRegistryClient()
            val serializer = SpecificAvro4kKafkaSerializer<BytesWrapper>(isKey = true, schemaRegistry = schemaRegistry)

            val bytes = serializer.serialize("topic", BytesWrapper(byteArrayOf(1, 2, 3)))

            val deserialized = GenericAvro4kKafkaDeserializer(isKey = true, schemaRegistry = schemaRegistry).deserialize("topic", bytes)
            deserialized shouldBe byteArrayOf(1, 2, 3)
        }

        "value class of ByteArray should be deserializable" {
            val schemaRegistry = MockSchemaRegistryClient()
            val bytes = GenericAvro4kKafkaSerializer(isKey = true, schemaRegistry = schemaRegistry).serialize("topic", byteArrayOf(1, 2, 3))
            val deserializer = SpecificAvro4kKafkaDeserializer(BytesWrapper.serializer(), isKey = true, schemaRegistry = schemaRegistry)

            val deserialized = deserializer.deserialize("topic", bytes)

            deserialized should beInstanceOf<BytesWrapper>()
            deserialized!!.value shouldBe byteArrayOf(1, 2, 3)
        }

        "enum should be deserializable from a serialized string" {
            val schemaRegistry = MockSchemaRegistryClient()
            val bytes = GenericAvro4kKafkaSerializer(isKey = true, schemaRegistry = schemaRegistry).serialize("topic", "B")
            val deserializer = SpecificAvro4kKafkaDeserializer(AnEnum.serializer(), isKey = true, schemaRegistry = schemaRegistry)

            val deserialized = deserializer.deserialize("topic", bytes)

            deserialized shouldBe AnEnum.B
        }
    }
}

@JvmInline
@Serializable
private value class StringWrapper(val value: String)

@JvmInline
@Serializable
private value class BytesWrapper(val value: ByteArray)

@Serializable
private enum class AnEnum {
    A,
    B,
}