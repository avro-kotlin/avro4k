package com.github.avrokotlin.avro4k.kafka.confluent

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.InternalAvro4kApi
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeSameInstanceAs
import io.mockk.every
import io.mockk.mockk
import kotlinx.serialization.DeserializationStrategy
import org.apache.avro.Schema

class AbstractAvro4kKafkaDeserializerTest : StringSpec() {
    // All the reflect/generic tests have been done in the KSerializer's tests
    init {
        "deserializing null returns null" {
            val deserializer = MockedDeserializer(Any())

            deserializer.deserialize("topic", null) shouldBe null
        }
        "deserializing should return the value from the KSerializer" {
            val value = Any()
            val deserializer = MockedDeserializer(value)

            deserializer.deserialize("topic", byteArrayOf(0, 0, 0, 0, 1)) shouldBeSameInstanceAs value
        }
    }
}

private class MockedDeserializer(
    decodedValue: Any,
    schemaRegistry: SchemaRegistryClient = MockSchemaRegistryClient(),
    avro: Avro = Avro,
    props: Map<String, *> = emptyMap<String, Any>(),
) : AbstractAvro4kKafkaDeserializer<Any>(avro) {
    override val deserializer: DeserializationStrategy<Any> = mockk<DeserializationStrategy<Any>>()

    @InternalAvro4kApi
    override val schema: Schema?
        get() = null

    init {
        every { deserializer.deserialize(any()) } returns decodedValue
        // The schema doesn't matter, as we mock the deserializer, so the decoding is not following the schema
        schemaRegistry.register("topic-value", AvroSchema(Schema.create(Schema.Type.LONG)))
        initialize(
            schemaRegistry,
            props,
            isKey = false
        )
    }
}