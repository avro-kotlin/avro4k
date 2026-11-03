package com.github.avrokotlin.avro4k.kafka.confluent

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.kotest.assertions.throwables.shouldThrowAny
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

        "an payload written with a different schema than the reader one should be deserializable" {
            @Serializable
            data class Writer(val a: Int, val b: String)

            @Serializable
            data class Reader(val b: String)

            val schemaRegistry = MockSchemaRegistryClient()
            val writerSerializer = SpecificAvro4kKafkaSerializer<Writer>(isKey = false, schemaRegistry = schemaRegistry)
            val bytes = writerSerializer.serialize("topic", Writer(42, "test"))
            val readerDeserializer = SpecificAvro4kKafkaDeserializer<Reader>(isKey = false, schemaRegistry = schemaRegistry)

            val deserialized = readerDeserializer.deserialize("topic", bytes)

            deserialized shouldBe Reader("test")
        }

        "a failed root string read should not break the next record read" {
            val schemaRegistry = MockSchemaRegistryClient()
            val stringBytes = GenericAvro4kKafkaSerializer(isKey = false, schemaRegistry = schemaRegistry).serialize("string-topic", "hello")!!
            val recordBytes = SpecificAvro4kKafkaSerializer<ExampleRecord>(isKey = false, schemaRegistry = schemaRegistry).serialize("record-topic", ExampleRecord("ok"))!!
            val deserializer = SpecificAvro4kKafkaDeserializer(ExampleRecord.serializer(), isKey = false, schemaRegistry = schemaRegistry)

            shouldThrowAny { deserializer.deserialize("string-topic", stringBytes.copyOfRange(0, stringBytes.size - 3)) }

            deserializer.deserialize("record-topic", recordBytes) shouldBe ExampleRecord("ok")
        }

        "serde should be able to serialize and deserialize a record" {
            val serde = SpecificAvro4kKafkaSerde<ExampleRecord>(
                isKey = false,
                props = mapOf("schema.registry.url" to "mock://avro4k-repro")
            )

            val original = ExampleRecord("example-id")
            val bytes = serde.serializer().serialize("test-topic", original)
            val deserialized = serde.deserializer().deserialize("test-topic", bytes)

            deserialized shouldBe original
        }
    }
}

@Serializable
private data class ExampleRecord(val id: String)

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