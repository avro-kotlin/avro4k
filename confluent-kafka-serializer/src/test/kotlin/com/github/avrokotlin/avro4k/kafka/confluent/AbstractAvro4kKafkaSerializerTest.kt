package com.github.avrokotlin.avro4k.kafka.confluent

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.internal.copy
import com.github.avrokotlin.avro4k.schema
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.NonRecordContainer
import io.kotest.assertions.throwables.shouldThrowAny
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerializationStrategy
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import java.nio.ByteBuffer

class AbstractAvro4kKafkaSerializerTest : StringSpec() {
    // All the reflect/generic tests have been done in the KSerializer's tests
    // Here we mostly check the schema inference
    init {
        "serializing null returns null" {
            val serializer = MockedSerializer()

            serializer.serialize("topic", null) shouldBe null
        }
        listOf<Any>(
            emptyList<Any>(),
            listOf(null),
            listOf(null, 12),
            listOf(12, null),

            emptySet<Any>(),
            setOf(null),
            setOf(null, 12),
            setOf(12, null),

            emptyArray<Any>(),
            arrayOf<Any?>(null),
            arrayOf(null, 12),
            arrayOf(12, null),

            emptyMap<Any, Any>(),
            mapOf("a" to null, "b" to 12),
            mapOf("a" to 12, "b" to null),
        ).forEach { value ->
            "serializing ${value::class.simpleName}($value) is not supported as the collection cannot contain nulls" {
                shouldThrowAny { GenericAvro4kKafkaSerializer(isKey = false, schemaRegistry = MockSchemaRegistryClient()).serialize("topic", value) }
            }
        }
        // The following seems "natural", but it infers the Schema from the value's type and not from static analysis as done in the core module
        listOf<Pair<Any, Schema>>(
            TestData("test", 24) to Avro.schema<TestData>(),
            SomeEnum.B to Avro.schema<SomeEnum>(),
            listOf("one", "two", 3) to Schema.createArray(Schema.create(Schema.Type.STRING)),
            setOf(1, 2.0, 3f) to Schema.createArray(Schema.create(Schema.Type.INT)),
            arrayOf(1, 2, 3) to Schema.createArray(Schema.create(Schema.Type.INT)),
            Utf8("string") to Schema.create(Schema.Type.STRING),
            "string" to Schema.create(Schema.Type.STRING),
            mapOf(15 to "str") to Avro.schema<Map<String, String>>(),
            'A' to Schema.create(Schema.Type.INT).copy(logicalTypeName = "char"),
            42 to Schema.create(Schema.Type.INT),
            42L to Schema.create(Schema.Type.LONG),
            3.14f to Schema.create(Schema.Type.FLOAT),
            3.14 to Schema.create(Schema.Type.DOUBLE),
            true to Schema.create(Schema.Type.BOOLEAN),
            byteArrayOf(1, 2, 3) to Schema.create(Schema.Type.BYTES),
            ByteBuffer.wrap(byteArrayOf(1, 2, 3)) to Schema.create(Schema.Type.BYTES),
            Avro.schema<SomeEnum>().let { GenericData.EnumSymbol(it, "B") to it },
            Avro.schema<TestData>().let { GenericData.Record(it).apply { put(0, "str"); put(1, 17) } to it },
            Schema.create(Schema.Type.STRING).let { NonRecordContainer(it, 42L) to it },
        ).forEach { (value, expectedSchema) ->
            "serializing ${value::class.simpleName}($value) registers schema $expectedSchema" {
                value shouldRegisterSchema expectedSchema
            }
        }
    }

    private infix fun Any.shouldRegisterSchema(
        expectedSchema: Schema,
    ) {
        val serializer = MockedSerializer()

        serializer.serialize("topic", this)

        serializer.schemaRegistry.allSubjects shouldBe listOf("topic-value")
        serializer.schemaRegistry.getAllVersions("topic-value") shouldBe listOf(1)
        serializer.schemaRegistry.getSchemaById(1).rawSchema() shouldBe expectedSchema
    }
}

@Serializable
private data class TestData(val name: String, val age: Int)

@Serializable
private enum class SomeEnum {
    A,
    B,
    C,
}

private class MockedSerializer(
    val schemaRegistry: SchemaRegistryClient = MockSchemaRegistryClient(),
    avro: Avro = Avro,
    props: Map<String, *> = emptyMap<String, Any>(),
) : AbstractAvro4kKafkaSerializer<Any>(avro) {
    override val serializer: SerializationStrategy<Any> = mockk<SerializationStrategy<Any>>()

    init {
        every { serializer.serialize(any(), any()) } returns Unit
        initialize(
            schemaRegistry,
            props,
            isKey = false
        )
    }
}