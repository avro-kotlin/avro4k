package com.github.avrokotlin.avro4k.kafka.confluent

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroDecoder
import com.github.avrokotlin.avro4k.AvroEncoder
import com.github.avrokotlin.avro4k.MissingFieldsEncodingException
import com.github.avrokotlin.avro4k.internal.copy
import com.github.avrokotlin.avro4k.kafka.confluent.GenericAvro4kSerializerTest.Companion.TOPIC_NAME
import com.github.avrokotlin.avro4k.kafka.confluent.KafkaSerializationTestHelpers.printWithType
import com.github.avrokotlin.avro4k.kafka.confluent.KafkaSerializationTestHelpers.registerSchema
import com.github.avrokotlin.avro4k.kafka.confluent.KafkaSerializationTestHelpers.schemaRegistryClient
import com.github.avrokotlin.avro4k.schema
import com.github.avrokotlin.avro4k.serializer.AvroSerializer
import com.github.avrokotlin.avro4k.serializer.SchemaSupplierContext
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import io.kotest.assertions.AssertionFailedError
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.assertions.withClue
import io.kotest.core.spec.style.FunSpec
import io.kotest.core.tuple
import io.kotest.matchers.EqualityMatcherResult
import io.kotest.matchers.Matcher
import io.kotest.matchers.MatcherResult
import io.kotest.matchers.be
import io.kotest.matchers.maps.haveKey
import io.kotest.matchers.maps.haveSize
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.string.shouldStartWith
import io.kotest.matchers.types.beInstanceOf
import io.kotest.matchers.types.beOfType
import kotlinx.serialization.Serializable
import org.apache.avro.LogicalType
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericArray
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericEnumSymbol
import org.apache.avro.generic.GenericFixed
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.util.Utf8
import org.apache.kafka.common.KafkaException
import java.math.BigInteger
import java.nio.ByteBuffer
import java.util.*

class GenericAvro4kSerializerTest : FunSpec({
    // All the native compatibility tests across types are in the Avro4k-core module

    test("serializing unknown class should throw an exception") {
        class UnregisteredClass
        try {
            serializeAutoRegistering(UnregisteredClass())
        } catch (e: Exception) {
            e should beInstanceOf<KafkaException>()
            e.message shouldStartWith "Cannot find serializer for type"
            e.message shouldContain UnregisteredClass::class.simpleName!!
        }
    }
    test("serializing serializable data class should be still serialized even if this is a generic serializer") {
        @Serializable
        data class Foo(val a: String, val b: Int)

        serializeAutoRegistering(Foo("hello", 42)) shouldBe serializedWithSchemaOfType(Schema.Type.RECORD)
    }
    context("serialized data is deserializable by confluent") {
        context("types in root level should be deserialized as expected") {

            // the following is a list of serialized data being deserialized the same by confluent
            listOf(
                null,
                true,
                42,
                42.0f,
                42.0,
                42L,
                "a text",
                byteArrayOf(4, 1, 5),
                GenericData.EnumSymbol(Schema.createEnum("TheEnum", null, null, listOf("A", "B")), "A"),
                GenericData.Fixed(Schema.createFixed("TheFixed", null, null, 2), byteArrayOf(4, 2)),
                GenericData.Array(Schema.createArray(Schema.create(Schema.Type.BOOLEAN)), listOf(true, false)),
                GenericData.Record(SchemaBuilder.record("TheRecord").fields().name("field").type(Schema.create(Schema.Type.INT)).withDefault(42).endRecord()).apply { put(0, 12) },
            ).forEach {
                test("${it.typeSimpleName} should be deserialized as ${it.typeSimpleName}") {
                    serializeAutoRegistering(it) shouldBe deserializedByConfluentAs(it)
                }
            }
            listOf<Pair<Any, Any>>(
                42.toByte() to 42,
                42.toUByte() to 42,
                42.toShort() to 42,
                42.toUShort() to 42,
                42U to 42,
                42UL to 42L,
                'Z' to 'Z'.code,
                Utf8("some text") to "some text",
                ByteBuffer.wrap(byteArrayOf(4, 1, 5)) to byteArrayOf(4, 1, 5),
                ByteBuffer.allocateDirect(3).put(byteArrayOf(4, 1, 5)).rewind() to byteArrayOf(4, 1, 5),
                BigInteger.valueOf(42) to "42",
                GenericMap(Schema.createMap(Schema.create(Schema.Type.INT)), mapOf(true to 42, false to 55.toShort())) to hashMapOf(Utf8("true") to 42, Utf8("false") to 55),
                GenericValue(Schema.createUnion(Schema.create(Schema.Type.BOOLEAN), Schema.create(Schema.Type.INT), Schema.create(Schema.Type.STRING)), 42) to 42,
            ).forEach {
                val (value, expected) = it
                test("${it.first::class.simpleName} should be deserialized as ${it.second::class.simpleName}") {
                    serializeAutoRegistering(value) shouldBe deserializedByConfluentAs(expected)
                }
            }
        }
        val nestedDataSet = listOf(
            // wrappedSchema, valueToSerialize, expectedDeserializedValue
            tuple(Schema.create(Schema.Type.NULL), null, null),
            tuple(Schema.create(Schema.Type.BOOLEAN), true, true),
            tuple(Schema.create(Schema.Type.INT), 42.toByte(), 42),
            tuple(Schema.create(Schema.Type.INT), 42.toUByte(), 42),
            tuple(Schema.create(Schema.Type.INT), 42.toShort(), 42),
            tuple(Schema.create(Schema.Type.INT), 42.toUShort(), 42),
            tuple(Schema.create(Schema.Type.INT), 42U, 42),
            tuple(Schema.create(Schema.Type.INT), 42, 42),
            tuple(Schema.create(Schema.Type.LONG), 42UL, 42L),
            tuple(Schema.create(Schema.Type.LONG), 42L, 42L),
            tuple(Schema.create(Schema.Type.FLOAT), 42.0f, 42.0f),
            tuple(Schema.create(Schema.Type.DOUBLE), 42.0, 42.0),
            tuple(Schema.create(Schema.Type.STRING), "a text", Utf8("a text")),
            tuple(Schema.create(Schema.Type.STRING), Utf8("a text"), Utf8("a text")),
            tuple(Schema.create(Schema.Type.BYTES), byteArrayOf(1, 2, 3), ByteBuffer.wrap(byteArrayOf(1, 2, 3))),
            tuple(Schema.create(Schema.Type.BYTES), ByteBuffer.wrap(byteArrayOf(1, 2, 3)), ByteBuffer.wrap(byteArrayOf(1, 2, 3))),
            tuple(Schema.create(Schema.Type.BYTES), ByteBuffer.allocateDirect(3).put(byteArrayOf(4, 1, 5)).rewind(), ByteBuffer.wrap(byteArrayOf(4, 1, 5))),
        )
        context("types wrapped in a map should be deserialized as the exact same") {
            nestedDataSet.forEach {
                val (wrappedSchema, valueToSerialize, expectedDeserializedValue) = it
                test("serializing ${valueToSerialize.typeSimpleName} should be deserialized as ${expectedDeserializedValue.typeSimpleName}") {
                    testWrappedInMap(wrappedSchema) {
                        serializeAutoRegistering(wrapValue(valueToSerialize)) shouldBe deserializedByConfluentAs(expectedDeserializedValue)
                    }
                }
            }
        }
        context("types wrapped in a record should be deserialized as expected") {
            nestedDataSet.forEach {
                val (wrappedSchema, valueToSerialize, expectedDeserializedValue) = it
                test("serializing ${valueToSerialize.typeSimpleName} should be deserialized as ${expectedDeserializedValue.typeSimpleName}") {
                    testWrappedInRecord(wrappedSchema) {
                        serializeAutoRegistering(wrapValue(valueToSerialize)) shouldBe deserializedByConfluentAs(expectedDeserializedValue)
                    }
                }
            }
        }
        context("types wrapped in an array should be deserialized as expected") {
            nestedDataSet.forEach {
                val (wrappedSchema, valueToSerialize, expectedDeserializedValue) = it
                test("serializing ${valueToSerialize.typeSimpleName} should be deserialized as ${expectedDeserializedValue.typeSimpleName}") {
                    testWrappedInArray(wrappedSchema) {
                        serializeAutoRegistering(wrapValue(valueToSerialize)) shouldBe deserializedByConfluentAs(expectedDeserializedValue)
                    }
                }
            }
        }
    }
    context("specific record use cases") {
        test("serializing a record while missing a required field should throw an exception") {
            val recordSchema = SchemaBuilder.record("TheRecord").fields()
                .name("field").type(Schema.create(Schema.Type.INT)).noDefault()
                .endRecord()
            shouldThrowCause<MissingFieldsEncodingException> {
                serializeAutoRegistering(GenericData.Record(recordSchema))
            }
        }
        test("serializing a record while missing a required nullable field should serialize null") {
            // we can't differentiate between a missing value and a null value
            val recordSchema = SchemaBuilder.record("TheRecord").fields()
                .name("field1").type(Schema.create(Schema.Type.BOOLEAN)).noDefault()
                .name("field2").type().nullable().intType().noDefault()
                .name("field3").type(Schema.create(Schema.Type.STRING)).noDefault()
                .endRecord()
            val serializedRecord = GenericData.Record(recordSchema).apply {
                put(0, true)
                put(2, "text")
            }
            val expectedRecord = GenericData.Record(recordSchema).apply {
                put(0, true)
                put(1, null)
                put(2, "text")
            }
            serializeAutoRegistering(serializedRecord) shouldBe deserializedByConfluentAs(expectedRecord)
        }
        test("serializing a record while missing an optional field should serialize the default") {
            val recordSchema = SchemaBuilder.record("TheRecord").fields()
                .name("field1").type(Schema.create(Schema.Type.BOOLEAN)).noDefault()
                .name("field2").type().intType().intDefault(17)
                .name("field3").type(Schema.create(Schema.Type.STRING)).noDefault()
                .endRecord()
            val serializedRecord = GenericData.Record(recordSchema).apply {
                put(0, true)
                put(2, "text")
            }
            val expectedRecord = GenericData.Record(recordSchema).apply {
                put(0, true)
                put(1, 17)
                put(2, "text")
            }
            serializeAutoRegistering(serializedRecord) shouldBe deserializedByConfluentAs(expectedRecord)
        }

        test("serializing a record while missing a required field from the already registered record should throw an exception") {
            val registeredRecordSchema = SchemaBuilder.record("TheRecord").fields()
                .name("field").type(Schema.create(Schema.Type.INT)).noDefault()
                .endRecord()
            val recordSchema = SchemaBuilder.record("TheRecord").fields()
                .endRecord()
            shouldThrowCause<MissingFieldsEncodingException> {
                serializeWithSpecificSchema(GenericData.Record(recordSchema), registeredRecordSchema)
            }
        }
        test("misaligned fields between serialized and writer schema") {
            val recordWriterSchema = SchemaBuilder.record("TheRecord").fields()
                .name("field3").type(Schema.create(Schema.Type.STRING)).noDefault()
                .name("field1").type(Schema.create(Schema.Type.BOOLEAN)).noDefault()
                .endRecord()
            val recordSchema = SchemaBuilder.record("TheRecord").fields()
                .name("field1").type(Schema.create(Schema.Type.BOOLEAN)).noDefault()
                .name("field2").type().intType().intDefault(17)
                .name("field3").type(Schema.create(Schema.Type.STRING)).noDefault()
                .endRecord()
            val serializedRecord = GenericData.Record(recordSchema).apply {
                put(0, true)
                put(1, 17)
                put(2, "text")
            }
            val expectedRecord = GenericData.Record(recordWriterSchema).apply {
                put(0, "text")
                put(1, true)
            }
            serializeWithSpecificSchema(serializedRecord, recordWriterSchema) shouldBe deserializedByConfluentAs(expectedRecord)
        }
        test("misaligned fields between serialized and writer schema") {
            val recordWriterSchema = SchemaBuilder.record("TheRecord").fields()
                .name("field3").type(Schema.create(Schema.Type.STRING)).noDefault()
                .name("field1").type(Schema.create(Schema.Type.BOOLEAN)).noDefault()
                .endRecord()
            val recordSchema = SchemaBuilder.record("TheRecord").fields()
                .name("field1").type(Schema.create(Schema.Type.BOOLEAN)).noDefault()
                .name("field2").type().intType().intDefault(17)
                .name("field3").type(Schema.create(Schema.Type.STRING)).noDefault()
                .endRecord()
            val serializedRecord = GenericData.Record(recordSchema).apply {
                put(0, true)
                put(1, 17)
                put(2, "text")
            }
            val expectedRecord = GenericData.Record(recordWriterSchema).apply {
                put(0, "text")
                put(1, true)
            }
            serializeWithSpecificSchema(serializedRecord, recordWriterSchema) shouldBe deserializedByConfluentAs(expectedRecord)
        }
    }
    context("aliases") {
        // TODO enum
        // TODO fixed
        // TODO record
    }
    context("auto-registering") {
        test("should register as boolean") {
            serializeAutoRegistering(true) shouldBe serializedWithSchema(Schema.create(Schema.Type.BOOLEAN))
        }
        test("serializing a char should register a int with char logical type") {
            serializeAutoRegistering('Z') shouldBe serializedWithSchema(Schema.create(Schema.Type.INT).copy(logicalType = LogicalType("char")))
        }
        context("should register as int") {
            listOf(
                42.toByte(),
                42.toUByte(),
                42.toShort(),
                42.toUShort(),
                42,
                42u,
            ).forEach {
                test(it::class.simpleName!!) {
                    serializeAutoRegistering(it) shouldBe serializedWithSchema(Schema.create(Schema.Type.INT))
                }
            }
        }
        context("should register as long") {
            listOf(42L, 42UL).forEach {
                test(it::class.simpleName!!) {
                    serializeAutoRegistering(it) shouldBe serializedWithSchema(Schema.create(Schema.Type.LONG))
                }
            }
        }
        test("should register as float") {
            serializeAutoRegistering(42.0f) shouldBe serializedWithSchema(Schema.create(Schema.Type.FLOAT))
        }
        test("should register as double") {
            serializeAutoRegistering(42.0) shouldBe serializedWithSchema(Schema.create(Schema.Type.DOUBLE))
        }
        context("should register as string") {
            listOf("a text", Utf8("a text"), BigInteger.valueOf(42)).forEach {
                test(it::class.simpleName!!) {
                    serializeAutoRegistering(it) shouldBe serializedWithSchema(Schema.create(Schema.Type.STRING))
                }
            }
        }
        context("should register as bytes") {
            listOf(byteArrayOf(1, 2, 3), ByteBuffer.wrap(byteArrayOf(1, 2, 3))).forEach {
                test(it::class.simpleName!!) {
                    serializeAutoRegistering(it) shouldBe serializedWithSchema(Schema.create(Schema.Type.BYTES))
                }
            }
        }
        context("serializing any GenericContainer should register its schema") {
            test(GenericRecord::class.simpleName!!) {
                val recordSchema = SchemaBuilder.record("TheRecord").fields()
                    .name("entry").type(Schema.create(Schema.Type.INT)).withDefault(42)
                    .endRecord()
                serializeAutoRegistering(GenericData.Record(recordSchema)) shouldBe serializedWithSchema(recordSchema)
            }
            test(GenericArray::class.simpleName!!) {
                val arraySchema = Schema.createArray(Schema.create(Schema.Type.BOOLEAN))
                serializeAutoRegistering(GenericData.Array(arraySchema, listOf(true, false))) shouldBe serializedWithSchema(arraySchema)
            }
            test(GenericFixed::class.simpleName!!) {
                val fixedSchema = Schema.createFixed("TheFixed", null, null, 2)
                serializeAutoRegistering(GenericData.Fixed(fixedSchema, byteArrayOf(4, 2))) shouldBe serializedWithSchema(fixedSchema)
            }
            test(GenericEnumSymbol::class.simpleName!!) {
                val enumSchema = Schema.createEnum("TheEnum", null, null, listOf("A", "B"), "B")
                serializeAutoRegistering(GenericData.EnumSymbol(enumSchema, "A")) shouldBe serializedWithSchema(enumSchema)
            }
            test(GenericMap::class.simpleName!!) {
                val mapSchema = Schema.createMap(Schema.create(Schema.Type.BOOLEAN))
                serializeAutoRegistering(GenericMap(mapSchema, mapOf("entry" to true))) shouldBe serializedWithSchema(mapSchema)
            }
            test(GenericValue::class.simpleName!!) {
                val explicitSchema = Schema.createUnion(Schema.create(Schema.Type.BOOLEAN), Schema.create(Schema.Type.INT), Schema.create(Schema.Type.STRING))
                serializeAutoRegistering(GenericValue(explicitSchema, 42)) shouldBe serializedWithSchema(explicitSchema)
            }
        }
        test("serializing a data class should register its schema") {
            @Serializable
            data class Foo(val a: String, val b: Int)
            serializeAutoRegistering(Foo("text", 42)) shouldBe serializedWithSchema(Avro.schema<Foo>())
        }
    }
}) {
    companion object {
        const val TOPIC_NAME = "test-record"

        private fun serializeAutoRegistering(
            value: Any?,
        ): SerializationResults? {
            val testId = UUID.randomUUID()
            // random isKey
            val isKey: Boolean = testId.hashCode() % 2 == 0
            val configs = mapOf<String, Any>(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to "mock://$testId",
                AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS to true,
            )
            val serializer = GenericAvro4kKafkaSerializer().apply { configure(configs, isKey) }
            val serializedBytes = serializer.serialize(TOPIC_NAME, value) ?: return null

            val writerSchemaId = extractSchemaId(serializedBytes)
            return SerializationResults(
                writerSchema = schemaRegistryClient(testId).getSchemaById(writerSchemaId).rawSchema() as Schema,
                writerSchemaId = writerSchemaId,
                serializedBytes = serializedBytes,
                schemaRegistryUrl = "mock://$testId",
                isKey = isKey,
            )
        }

        private fun serializeWithSpecificSchema(
            value: Any?,
            schema: Schema,
        ): SerializationResults? {
            val testId = UUID.randomUUID()
            // random isKey
            val isKey: Boolean = testId.hashCode() % 2 == 0
            val configs = mapOf<String, Any>(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to "mock://$testId",
                AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS to false,
                AbstractKafkaSchemaSerDeConfig.USE_SCHEMA_ID to registerSchema(testId, schema, isKey),
                KafkaAvroDeserializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG to true,
            )
            val serializer = GenericAvro4kKafkaSerializer().apply { configure(configs, isKey) }
            val serializedBytes = serializer.serialize(TOPIC_NAME, value) ?: return null

            val writerSchemaId = extractSchemaId(serializedBytes)
            return SerializationResults(
                writerSchema = schemaRegistryClient(testId).getSchemaById(writerSchemaId).rawSchema() as Schema,
                writerSchemaId = writerSchemaId,
                serializedBytes = serializedBytes,
                schemaRegistryUrl = "mock://$testId",
                isKey = isKey,
            )
        }

        private fun beSameFixedAs(expected: GenericFixed) = object : Matcher<Any?> {
            override fun test(value: Any?): MatcherResult {
                if (value !is GenericFixed) return MatcherResult(false, { "Not a fixed" }, { "Not a fixed" })
                return MatcherResult(
                    value.bytes().contentEquals(expected.bytes()) && value.schema == expected.schema,
                    { "Fixed is different" },
                    { "Fixed is the same" }
                )
            }
        }

        @Serializable(with = RainyLogicalTypeWrapper.Companion.Serializer::class)
        private data class RainyLogicalTypeWrapper(val isRainy: Boolean) {
            companion object {
                val logicalTypeSchema = Schema.create(Schema.Type.STRING).copy(logicalType = LogicalType("custom-type"))

                internal class Serializer : AvroSerializer<RainyLogicalTypeWrapper>(RainyLogicalTypeWrapper::class.qualifiedName!!) {
                    override fun serializeAvro(encoder: AvroEncoder, value: RainyLogicalTypeWrapper) {
                        encoder.encodeString(if (value.isRainy) "raining" else "sunny")
                    }

                    override val supportedLogicalTypes: Set<String>
                        get() = setOf("custom-type")

                    override fun deserializeAvro(decoder: AvroDecoder): RainyLogicalTypeWrapper {
                        TODO("Not yet implemented")
                    }

                    override fun getSchema(context: SchemaSupplierContext): Schema {
                        return logicalTypeSchema
                    }
                }
            }
        }
    }
}

private fun <T> onThrowMatcher(test: (value: T) -> Unit) = Matcher<T> { actualValue ->
    try {
        test(actualValue)
        SuccessMatcherResult
    } catch (e: AssertionFailedError) {
        MatcherErrorResult(
            actual = e.actual?.ephemeralValue,
            expected = e.expected?.ephemeralValue,
            e.message ?: "Assertion failed:\n${e.printStackTrace()}",
        )
    } catch (e: AssertionError) {
        MatcherErrorResult(e.message ?: "Assertion failed:\n${e.printStackTrace()}")
    }

}

private fun MatcherErrorResult(
    failureMessage: String,
): MatcherResult {
    return MatcherResult(false, { failureMessage })
}

private fun MatcherResult(
    passed: Boolean,
    failureMessage: () -> String,
): MatcherResult {
    return MatcherResult(passed, failureMessage, { throw UnsupportedOperationException() })
}

private fun MatcherErrorResult(
    actual: Any?,
    expected: Any?,
    failureMessage: String,
): MatcherResult {
    return EqualityMatcherResult(false, actual, expected, { failureMessage }, { throw UnsupportedOperationException() })
}

private object SuccessMatcherResult : MatcherResult {
    override fun failureMessage(): String = throw UnsupportedOperationException()
    override fun negatedFailureMessage(): String = throw UnsupportedOperationException()
    override fun passed(): Boolean = true
}

class SerializationResults(
    val writerSchema: Schema,
    val writerSchemaId: Int,
    val serializedBytes: ByteArray,
    val isKey: Boolean,
    val schemaRegistryUrl: String,
)

private fun extractSchemaId(serializedBytes: ByteArray): Int {
    return serializedBytes.copyOfRange(1, 5).let { ByteBuffer.wrap(it) }.int
}

private fun serializedWithSchema(expected: Schema) = onThrowMatcher<SerializationResults?> {
    withClue("The writer-schema is not the expected one") {
        it?.writerSchema shouldBe expected
    }
}

private fun serializedWithSchemaOfType(expected: Schema.Type) = onThrowMatcher<SerializationResults?> {
    withClue("The writer-schema is not the expected one") {
        it?.writerSchema?.type shouldBe expected
    }
}

private fun serializedWithSchemaId(expectedSchemaId: Int) = onThrowMatcher<SerializationResults?> {
    withClue("The writer-schema id is not the expected one") {
        it?.writerSchemaId shouldBe expectedSchemaId
    }
}

private fun deserializedByConfluentAs(expected: Any?) = onThrowMatcher<SerializationResults?> {
    val deserializedValue = if (it == null) null else deserializeWithConfluent(it)

    withClue("The deserialized value ${deserializedValue.printWithType()} is not the expected ${expected.printWithType()}") {
        if (expected != null) {
            deserializedValue should beOfType(expected::class)
        }
        deserializedValue should be(expected)
    }
}

private fun deserializeWithConfluent(it: SerializationResults): Any? = KafkaAvroDeserializer().apply {
    configure(
        mapOf<String, Any>(
            KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG to it.schemaRegistryUrl,
            KafkaAvroDeserializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG to true,
        ),
        it.isKey
    )
}.deserialize(TOPIC_NAME, it.serializedBytes)

interface WrappingTestData {
    val wrappedSchema: Schema
    val wrappingSchema: Schema
    fun unwrapValue(deserializedValue: Any?): Any?
    fun wrapValue(serializedValue: Any?): Any
}

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

private fun WrappingTestData.deserializedByConfluentAs(expectedWrappedValue: Any?) = onThrowMatcher<SerializationResults?> {
    val deserializedValue = if (it == null) null else deserializeWithConfluent(it)

    val serializedWrappedValue = unwrapValue(deserializedValue)
    withClue("The deserialized value ${deserializedValue.printWithType()} is not the expected wrapped value ${expectedWrappedValue.printWithType()} in record") {
        if (expectedWrappedValue != null) {
            serializedWrappedValue should beOfType(expectedWrappedValue::class)
        }
        serializedWrappedValue should be(expectedWrappedValue)
    }
}

private val Any?.typeSimpleName: String get() = if (this == null) "null" else this::class.simpleName!!

inline fun <reified T : Throwable> shouldThrowCause(block: () -> Any?) {
    val thrownCause = try {
        block()
        null
    } catch (thrown: Throwable) {
        thrown.cause
    }
    withClue("The block should throw an exception with cause of type ${T::class.qualifiedName}") {
        shouldThrow<T> { if (thrownCause != null) throw thrownCause }
    }
}

inline fun <reified T : Throwable> shouldThrowCauseWithMessageContaining(messagePart: String, block: () -> Any?) {
    val thrownCause = try {
        block()
        null
    } catch (thrown: Throwable) {
        thrown.cause
    }
    withClue("The block should throw an exception with cause of type ${T::class.qualifiedName} with message containing $messagePart") {
        shouldThrow<T>() { if (thrownCause != null) throw thrownCause }
        thrownCause?.message shouldContain messagePart
    }
}
