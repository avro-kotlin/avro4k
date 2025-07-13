package com.github.avrokotlin.avro4k.kafka.confluent

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.MissingFieldsEncodingException
import com.github.avrokotlin.avro4k.internal.copy
import com.github.avrokotlin.avro4k.schema
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.kotest.core.spec.style.FunSpec
import io.kotest.core.tuple
import io.kotest.matchers.and
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerializationException
import org.apache.avro.LogicalType
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericArray
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericEnumSymbol
import org.apache.avro.generic.GenericFixed
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import java.math.BigInteger
import java.nio.ByteBuffer
import java.util.UUID

class GenericAvro4kSerializerTest : FunSpec({
    test("serializing unknown class should throw an exception") {
        class UnregisteredClass
        shouldThrowCauseWithMessageContaining<SerializationException>("Serializer for class 'UnregisteredClass' is not found") {
            serializeAutoRegistering(UnregisteredClass())
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
                        serializeAutoRegistering(wrapValue(valueToSerialize)) shouldBe
                                (serializedWithSchema(wrappingSchema) and deserializedByConfluentAs(expectedDeserializedValue))
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
        test("serializing a record while missing an optional field should fail") {
            val recordSchema = SchemaBuilder.record("TheRecord").fields()
                .name("field1").type(Schema.create(Schema.Type.BOOLEAN)).noDefault()
                .name("field2").type().intType().intDefault(17)
                .name("field3").type(Schema.create(Schema.Type.STRING)).noDefault()
                .endRecord()
            val serializedRecord = GenericData.Record(recordSchema).apply {
                put(0, true)
                put(2, "text")
            }
            shouldThrowCause<MissingFieldsEncodingException> {
                serializeAutoRegistering(serializedRecord)
            }
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
        test("misaligned fields between serialized and writer schema with different order should work") {
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
                    .name("entry").type(Schema.create(Schema.Type.INT)).noDefault()
                    .endRecord()
                serializeAutoRegistering(GenericData.Record(recordSchema).apply { this.put(0, 42) }) shouldBe serializedWithSchema(recordSchema)
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
//    TODO("test logical types and their config")
}) {
    companion object {
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

        private fun serializeUsingLatest(
            value: Any?,
            schema: Schema,
        ): SerializationResults? {
            val testId = UUID.randomUUID()
            // random isKey
            val isKey: Boolean = testId.hashCode() % 2 == 0

            // register another schema before to ensure an older schema isn't used
            registerSchema(testId, Schema.createEnum("None", null, "unknown.package", listOf()), isKey)

            registerSchema(testId, schema, isKey)

            val configs = mapOf<String, Any>(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to "mock://$testId",
                AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS to false,
                AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION to true,
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

        private fun serializeUsingMatchingSchema(
            value: Any?,
            schema: Schema,
        ): SerializationResults? {
            val testId = UUID.randomUUID()
            // random isKey
            val isKey: Boolean = testId.hashCode() % 2 == 0

            // register another schema before and after to ensure the used schema is the one matching the serialized data
            registerSchema(testId, Schema.createEnum("None", null, "unknown.package", listOf()), isKey)
            registerSchema(testId, schema, isKey)
            registerSchema(testId, Schema.createEnum("None2", null, "unknown.package", listOf()), isKey)

            val configs = mapOf<String, Any>(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to "mock://$testId",
                AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS to false,
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
    }
}
