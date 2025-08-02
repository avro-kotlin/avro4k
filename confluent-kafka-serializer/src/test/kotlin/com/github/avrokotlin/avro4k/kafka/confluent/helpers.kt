package com.github.avrokotlin.avro4k.kafka.confluent

import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import io.kotest.assertions.withClue
import io.kotest.matchers.be
import io.kotest.matchers.maps.haveKey
import io.kotest.matchers.maps.haveSize
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.beInstanceOf
import io.kotest.matchers.types.beOfType
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.util.Utf8
import java.nio.ByteBuffer
import java.util.*
import kotlin.math.absoluteValue


fun serializedWithSchema(expected: Schema) = onThrowMatcher<SerializationResults?> {
    withClue("The writer-schema is not the expected one") {
        it?.writerSchema shouldBe expected
    }
}

fun serializedWithSchemaOfType(expected: Schema.Type) = onThrowMatcher<SerializationResults?> {
    withClue("The writer-schema is not the expected one") {
        it?.writerSchema?.type shouldBe expected
    }
}

fun serializedWithSchemaId(expectedSchemaId: Int) = onThrowMatcher<SerializationResults?> {
    withClue("The writer-schema id is not the expected one") {
        it?.writerSchemaId shouldBe expectedSchemaId
    }
}

 fun deserializedByConfluentAs(expected: Any?) = onThrowMatcher<SerializationResults?> {
    val deserializedValue = if (it == null) null else deserializeWithConfluent(it)

    withClue("The deserialized value ${deserializedValue.printWithType()} is not the expected ${expected.printWithType()}") {
        if (expected != null) {
            deserializedValue should beOfType(expected::class)
        }
        deserializedValue should be(expected)
    }
}

class SerializationResults(
    val writerSchema: Schema,
    val writerSchemaId: Int,
    val serializedBytes: ByteArray,
    val isKey: Boolean,
    val schemaRegistryUrl: String,
)

private fun deserializeWithConfluent(it: SerializationResults): Any? = KafkaAvroDeserializer().apply {
    configure(
        mapOf<String, Any>(
            KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG to it.schemaRegistryUrl,
            KafkaAvroDeserializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG to true,
        ),
        it.isKey
    )
}.deserialize(TOPIC_NAME, it.serializedBytes)

fun testWrappedInRecord(wrappedSchema: Schema, assertion: WrappingTestData.() -> Unit) {
    WrappedInRecordTestScope(wrappedSchema).assertion()
}

class WrappedInRecordTestScope(override val wrappedSchema: Schema) : WrappingTestData {
    override val wrappingSchema: Schema = SchemaBuilder.record("TestRecord").fields()
        .name("field").type(wrappedSchema).noDefault()
        .endRecord()

    override fun unwrapValue(deserializedValue: Any?): Any? {
        withClue("The deserialized value is not a record") {
            deserializedValue should beInstanceOf<IndexedRecord>()
        }
        deserializedValue as IndexedRecord
        withClue("The deserialized record's schema is not the expected schema $wrappingSchema. Actual schema: ${deserializedValue.schema}") {
            deserializedValue.schema shouldBe wrappingSchema
        }
        return deserializedValue[0]
    }

    override fun wrapValue(serializedValue: Any?): Any {
        return GenericData.Record(wrappingSchema).apply { put(0, serializedValue) }
    }
}

fun testWrappedInMap(wrappedSchema: Schema, assertion: WrappingTestData.() -> Unit) {
    WrappedInMapTestScope(wrappedSchema).assertion()
}

class WrappedInMapTestScope(override val wrappedSchema: Schema) : WrappingTestData {
    override val wrappingSchema: Schema = Schema.createMap(wrappedSchema)

    override fun unwrapValue(deserializedValue: Any?): Any? {
        withClue("The deserialized value is not a map") {
            deserializedValue should beInstanceOf<Map<*, *>>()
        }
        @Suppress("UNCHECKED_CAST")
        deserializedValue as Map<Utf8, Any?>
        withClue("The deserialized map should have one specific entry. Actual map: $deserializedValue") {
            deserializedValue should haveSize(1)
            deserializedValue should haveKey(Utf8("entry"))
        }
        return deserializedValue.entries.first().value
    }

    override fun wrapValue(serializedValue: Any?): Any {
        return GenericMap(wrappingSchema, mapOf("entry" to serializedValue))
    }
}

fun testWrappedInArray(wrappedSchema: Schema, assertion: WrappingTestData.() -> Unit) {
    WrappedInArrayTestScope(wrappedSchema).assertion()
}

class WrappedInArrayTestScope(override val wrappedSchema: Schema) : WrappingTestData {
    override val wrappingSchema: Schema = Schema.createArray(wrappedSchema)

    override fun unwrapValue(deserializedValue: Any?): Any? {
        withClue("The deserialized value is not an array") {
            deserializedValue should beInstanceOf<List<*>>()
        }
        deserializedValue as List<Any?>
        withClue("The deserialized array should have exactly one item. Actual array: $deserializedValue") {
            deserializedValue should io.kotest.matchers.collections.haveSize(1)
        }
        return deserializedValue.first()
    }

    override fun wrapValue(serializedValue: Any?): Any {
        return GenericData.Array(wrappingSchema, listOf(serializedValue))
    }
}

fun WrappingTestData.deserializedByConfluentAs(expectedWrappedValue: Any?) = onThrowMatcher<SerializationResults?> {
    val deserializedValue = if (it == null) null else deserializeWithConfluent(it)

    val serializedWrappedValue = unwrapValue(deserializedValue)
    withClue("The deserialized value ${deserializedValue.printWithType()} is not the expected wrapped value ${expectedWrappedValue.printWithType()} in record") {
        if (expectedWrappedValue != null) {
            serializedWrappedValue should beOfType(expectedWrappedValue::class)
        }
        serializedWrappedValue should be(expectedWrappedValue)
    }
}

interface WrappingTestData {
    val wrappedSchema: Schema
    val wrappingSchema: Schema
    fun unwrapValue(deserializedValue: Any?): Any?
    fun wrapValue(serializedValue: Any?): Any
}

val Any?.typeSimpleName: String get() = if (this == null) "null" else this::class.simpleName!!

const val TOPIC_NAME = "test-record"

fun registerSchema(testId: UUID, schema: Schema, isKey: Boolean): Int {
    val schemaId = testId.hashCode().absoluteValue
    val keyOrValue = if (isKey) "key" else "value"
    schemaRegistryClient(testId).register("$TOPIC_NAME-$keyOrValue", AvroSchema(schema), -1 /*useless value for tests*/, schemaId)
    return schemaId
}

fun schemaRegistryClient(testId: UUID): SchemaRegistryClient =
    SchemaRegistryClientFactory.newClient(listOf("mock://$testId"), 100, listOf(AvroSchemaProvider()), emptyMap<String, Any>(), emptyMap())

private fun Any?.printWithType(): String {
    if (this == null) return "<null>"
    return "${this::class.simpleName}<$this>"
}

fun extractSchemaId(serializedBytes: ByteArray): Int {
    return serializedBytes.copyOfRange(1, 5).let { ByteBuffer.wrap(it) }.int
}
