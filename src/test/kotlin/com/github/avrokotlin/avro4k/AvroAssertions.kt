package com.github.avrokotlin.avro4k

import com.github.avrokotlin.avro4k.internal.nullable
import io.kotest.assertions.Actual
import io.kotest.assertions.Expected
import io.kotest.assertions.failure
import io.kotest.assertions.print.Printed
import io.kotest.assertions.withClue
import io.kotest.core.spec.style.scopes.StringSpecRootScope
import io.kotest.matchers.shouldBe
import kotlinx.serialization.KSerializer
import kotlinx.serialization.serializer
import org.apache.avro.Conversions
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericContainer
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.nio.file.Path

internal class AvroEncodingAssertions<T>(
    private val valueToEncode: T,
    private val serializer: KSerializer<T>,
) {
    private var avro: Avro =
        Avro {
            validateSerialization = true
        }

    fun withConfig(builder: AvroBuilder.() -> Unit): AvroEncodingAssertions<T> {
        this.avro = Avro(from = avro, builderAction = builder)
        return this
    }

    fun generatesSchema(expectedSchema: Schema): AvroEncodingAssertions<T> {
        avro.schema(serializer).toString(true) shouldBe expectedSchema.toString(true)
        return this
    }

    fun generatesSchema(
        expectedSchemaResourcePath: Path,
        schemaTransformer: (Schema) -> Schema = { it },
    ): AvroEncodingAssertions<T> {
        generatesSchema(Schema.Parser().parse(javaClass.getResourceAsStream(expectedSchemaResourcePath.toString())).let(schemaTransformer))
        return this
    }

    fun isEncodedAs(
        expectedEncodedGenericValue: Any?,
        expectedDecodedValue: T = valueToEncode,
        writerSchema: Schema = avro.schema(serializer),
        decodedComparator: (actual: T, expected: T) -> Unit = { a, b -> a shouldBe b },
    ): AvroEncodingAssertions<T> {
        val actualEncodedBytes = avro4kEncode(valueToEncode, writerSchema)
        val apacheEncodedBytes = avroApacheEncode(expectedEncodedGenericValue, writerSchema)
        withClue("Encoded bytes are not the same as apache avro library.") {
            if (!actualEncodedBytes.contentEquals(apacheEncodedBytes)) {
                val expectedAvroJson = bytesToAvroJson(apacheEncodedBytes, writerSchema)
                val actualAvroJson = bytesToAvroJson(actualEncodedBytes, writerSchema)
                throw failure(Expected(Printed(expectedAvroJson)), Actual(Printed(actualAvroJson)))
            }
        }

        val actualGenericData = normalizeGenericData(avro4kGenericEncode(valueToEncode, writerSchema))
        val normalizeGenericData = normalizeGenericData(expectedEncodedGenericValue)
        withClue("Encoded generic data is not the same as the expected one.") {
            actualGenericData shouldBe normalizeGenericData
        }

        val decodedValue = avro4kDecode(apacheEncodedBytes, writerSchema, serializer)
        withClue("Decoded value is not the same as the expected one.") {
            decodedComparator(decodedValue, expectedDecodedValue)
        }
        return this
    }

    inline fun <reified R> isDecodedAs(
        expected: R,
        serializer: KSerializer<R> = avro.serializersModule.serializer<R>(),
        writerSchema: Schema = avro.schema(this.serializer),
    ) {
        val encodedBytes = avro4kEncode(valueToEncode, writerSchema)

        val decodedValue = avro4kDecode(encodedBytes, writerSchema, serializer)
        withClue("Decoded value is not the same as the expected one.") {
            decodedValue shouldBe expected
        }
    }

    private fun avro4kEncode(
        value: T,
        schema: Schema,
    ): ByteArray {
        return avro.encodeToByteArray(schema, serializer, value)
    }

    private fun avro4kGenericEncode(
        value: T,
        schema: Schema,
    ): Any? {
        return avro.encodeToGenericData(schema, serializer, value)
    }

    private fun <R> avro4kDecode(
        bytes: ByteArray,
        writerSchema: Schema,
        serializer: KSerializer<R>,
    ): R {
        return avro.decodeFromByteArray(writerSchema, serializer, bytes)
    }

    private fun avroApacheEncode(
        value: Any?,
        writerSchema: Schema,
    ): ByteArray {
        val writer =
            GenericDatumWriter<Any>(
                writerSchema,
                GenericData().apply {
                    addLogicalTypeConversion(Conversions.UUIDConversion())
                    addLogicalTypeConversion(Conversions.DecimalConversion())
                }
            )
        val byteArrayOutputStream = ByteArrayOutputStream()
        val encoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null)
        writer.write(convertToAvroGenericValue(value, writerSchema), encoder)
        encoder.flush()
        return byteArrayOutputStream.toByteArray()
    }

    private fun bytesToAvroJson(
        bytes: ByteArray,
        schema: Schema,
    ): String {
        return avroApacheEncodeJson(avroApacheDecode(bytes, schema), schema)
    }

    private fun avroApacheDecode(
        bytes: ByteArray,
        schema: Schema,
    ): Any? {
        val reader = GenericDatumReader<Any>(schema)
        val decoder = DecoderFactory.get().binaryDecoder(ByteArrayInputStream(bytes), null)
        return reader.read(null, decoder)
    }

    private fun avroApacheEncodeJson(
        value: Any?,
        schema: Schema,
    ): String {
        val writer = GenericDatumWriter<Any>(schema)
        val byteArrayOutputStream = ByteArrayOutputStream()
        val encoder = EncoderFactory.get().jsonEncoder(schema, byteArrayOutputStream, true)
        writer.write(convertToAvroGenericValue(value, schema), encoder)
        encoder.flush()
        return byteArrayOutputStream.toByteArray().decodeToString()
    }
}

internal open class AvroSchemaAssertions<T>(
    private val serializer: KSerializer<T>,
    private var avro: Avro = Avro {},
) {
    private lateinit var generatedSchema: Schema

    fun withConfig(builder: AvroBuilder.() -> Unit): AvroSchemaAssertions<T> {
        this.avro = Avro(builderAction = builder)
        return this
    }

    fun generatesSchema(expectedSchema: Schema) {
        avro.schema(serializer).toString(true) shouldBe expectedSchema.toString(true)
        generatedSchema = expectedSchema
    }

    fun generatesSchema(
        expectedSchemaResourcePath: Path,
        schemaTransformer: (Schema) -> Schema = { it },
    ) {
        generatesSchema(Schema.Parser().parse(javaClass.getResourceAsStream(expectedSchemaResourcePath.toString())).let(schemaTransformer))
    }
}

internal object AvroAssertions {
    inline fun <reified T> assertThat(): AvroSchemaAssertions<T> {
        return AvroSchemaAssertions(Avro.serializersModule.serializer<T>())
    }

    fun <T> assertThat(serializer: KSerializer<T>): AvroSchemaAssertions<T> {
        return AvroSchemaAssertions(serializer)
    }

    inline fun <reified T> assertThat(value: T): AvroEncodingAssertions<T> {
        return AvroEncodingAssertions(value, Avro.serializersModule.serializer<T>())
    }

    @Suppress("UNCHECKED_CAST")
    inline fun <reified T> assertThat(
        value: T,
        serializer: KSerializer<out T>,
    ): AvroEncodingAssertions<T> {
        return AvroEncodingAssertions(value, serializer as KSerializer<T>)
    }
}

fun encodeToBytesUsingApacheLib(
    schema: Schema,
    toEncode: Any?,
): ByteArray {
    return ByteArrayOutputStream().use {
        GenericData.get().createDatumWriter(schema).write(toEncode, EncoderFactory.get().directBinaryEncoder(it, null))
        it.toByteArray()
    }
}

internal inline fun <reified T> StringSpecRootScope.basicScalarEncodeDecodeTests(value: T, schema: Schema, apacheCompatibleValue: Any? = value) {
    "support scalar type ${schema.type} serialization" {
        Avro.schema<T>() shouldBe schema
        testEncodeDecode(schema, value, apacheCompatibleValue = apacheCompatibleValue)

        Avro.schema<ValueClassWithGenericField<T>>() shouldBe schema
        testEncodeDecode(schema, ValueClassWithGenericField(value), apacheCompatibleValue = apacheCompatibleValue)
    }
    "support scalar type ${schema.type} serialization as nullable" {
        Avro.schema<T?>() shouldBe schema.nullable
        testEncodeDecode<T?>(schema.nullable, value, apacheCompatibleValue = apacheCompatibleValue)
        testEncodeDecode<T?>(schema.nullable, null)

        Avro.schema<ValueClassWithGenericField<T?>>() shouldBe schema.nullable
        testEncodeDecode(schema.nullable, ValueClassWithGenericField<T?>(value), apacheCompatibleValue = apacheCompatibleValue)
        testEncodeDecode(schema.nullable, ValueClassWithGenericField<T?>(null), apacheCompatibleValue = null)

        Avro.schema<ValueClassWithGenericField<T?>?>() shouldBe schema.nullable
        testEncodeDecode<ValueClassWithGenericField<T?>?>(schema.nullable, null)

        Avro.schema<ValueClassWithGenericField<T>?>() shouldBe schema.nullable
        testEncodeDecode<ValueClassWithGenericField<T>?>(schema.nullable, null)
    }
    "scalar type ${schema.type} in record" {
        val record =
            SchemaBuilder.record("RecordWithGenericField").fields()
                .name("field").type(schema).noDefault()
                .endRecord()
        Avro.schema<RecordWithGenericField<T>>() shouldBe record
        Avro.schema<RecordWithGenericField<ValueClassWithGenericField<T>>>() shouldBe record
        testEncodeDecode(record,
            RecordWithGenericField(value), apacheCompatibleValue = GenericData.Record(record).also { it.put(0, apacheCompatibleValue) })
        testEncodeDecode(record,
            RecordWithGenericField(ValueClassWithGenericField(value)), apacheCompatibleValue = GenericData.Record(record).also { it.put(0, apacheCompatibleValue) })
    }
    "scalar type ${schema.type} in record as nullable field" {
        val expectedRecordSchemaNullable =
            SchemaBuilder.record("RecordWithGenericField").fields()
                .name("field").type(schema.nullable).withDefault(null)
                .endRecord()
        Avro.schema<RecordWithGenericField<T?>>() shouldBe expectedRecordSchemaNullable
        Avro.schema<RecordWithGenericField<ValueClassWithGenericField<T?>>>() shouldBe expectedRecordSchemaNullable
        Avro.schema<RecordWithGenericField<ValueClassWithGenericField<T?>?>>() shouldBe expectedRecordSchemaNullable
        Avro.schema<RecordWithGenericField<ValueClassWithGenericField<T>?>>() shouldBe expectedRecordSchemaNullable

        val recordNullable =
            SchemaBuilder.record("RecordWithGenericField").fields()
                .name("field").type(schema.nullable).noDefault()
                .endRecord()
        testEncodeDecode(recordNullable,
            RecordWithGenericField<T?>(value), apacheCompatibleValue = GenericData.Record(recordNullable).also { it.put(0, apacheCompatibleValue) })
        testEncodeDecode(recordNullable,
            RecordWithGenericField<T?>(null), apacheCompatibleValue = GenericData.Record(recordNullable).also { it.put(0, null) })
        testEncodeDecode(
            recordNullable,
            RecordWithGenericField(ValueClassWithGenericField<T?>(value)),
            apacheCompatibleValue = GenericData.Record(recordNullable).also { it.put(0, apacheCompatibleValue) }
        )
        testEncodeDecode(recordNullable,
            RecordWithGenericField(ValueClassWithGenericField<T?>(null)), apacheCompatibleValue = GenericData.Record(recordNullable).also { it.put(0, null) })
    }
    "scalar type ${schema.type} in map" {
        val map = SchemaBuilder.map().values(schema)
        Avro.schema<Map<String, T>>() shouldBe map
        Avro.schema<Map<String, ValueClassWithGenericField<T>>>() shouldBe map
        Avro.schema<Map<T, ValueClassWithGenericField<T>>>() shouldBe map
        Avro.schema<Map<ValueClassWithGenericField<T>, ValueClassWithGenericField<T>>>() shouldBe map
        Avro.schema<Map<T, T>>() shouldBe map
        Avro.schema<Map<ValueClassWithGenericField<T>, T>>() shouldBe map
        testEncodeDecode(map, mapOf("key" to value), apacheCompatibleValue = mapOf("key" to apacheCompatibleValue))
        testEncodeDecode(map, mapOf("key" to ValueClassWithGenericField(value)), apacheCompatibleValue = mapOf("key" to apacheCompatibleValue))

        val mapNullable = SchemaBuilder.map().values(schema.nullable)
        Avro.schema<Map<String, T?>>() shouldBe mapNullable
        Avro.schema<Map<String, ValueClassWithGenericField<T?>>>() shouldBe mapNullable
        Avro.schema<Map<String, ValueClassWithGenericField<T?>?>>() shouldBe mapNullable
        Avro.schema<Map<String, ValueClassWithGenericField<T>?>>() shouldBe mapNullable
        Avro.schema<Map<T, ValueClassWithGenericField<T?>>>() shouldBe mapNullable
        Avro.schema<Map<T, ValueClassWithGenericField<T?>?>>() shouldBe mapNullable
        Avro.schema<Map<T, ValueClassWithGenericField<T>?>>() shouldBe mapNullable
        Avro.schema<Map<ValueClassWithGenericField<T>, ValueClassWithGenericField<T?>>>() shouldBe mapNullable
        Avro.schema<Map<ValueClassWithGenericField<T>, ValueClassWithGenericField<T?>?>>() shouldBe mapNullable
        Avro.schema<Map<ValueClassWithGenericField<T>, ValueClassWithGenericField<T>?>>() shouldBe mapNullable
        Avro.schema<Map<T, T?>>() shouldBe mapNullable
        Avro.schema<Map<ValueClassWithGenericField<T>, T?>>() shouldBe mapNullable
        testEncodeDecode(mapNullable, mapOf("key" to ValueClassWithGenericField<T?>(value)), apacheCompatibleValue = mapOf("key" to apacheCompatibleValue))
        testEncodeDecode(mapNullable, mapOf("key" to ValueClassWithGenericField<T?>(null)), apacheCompatibleValue = mapOf("key" to null))
    }
    "scalar type ${schema.type} in array" {
        val array = SchemaBuilder.array().items(schema)
        Avro.schema<List<T>>() shouldBe array
        Avro.schema<List<ValueClassWithGenericField<T>>>() shouldBe array
        Avro.schema<Set<T>>() shouldBe array
        Avro.schema<Set<ValueClassWithGenericField<T>>>() shouldBe array
        Avro.schema<Array<T>>() shouldBe array
        Avro.schema<Array<ValueClassWithGenericField<T>>>() shouldBe array
        testEncodeDecode(array, listOf(value), apacheCompatibleValue = listOf(apacheCompatibleValue))
        testEncodeDecode(array, listOf(ValueClassWithGenericField(value)), apacheCompatibleValue = listOf(apacheCompatibleValue))

        val arrayNullable = SchemaBuilder.array().items(schema.nullable)
        Avro.schema<List<T?>>() shouldBe arrayNullable
        Avro.schema<List<ValueClassWithGenericField<T?>>>() shouldBe arrayNullable
        Avro.schema<List<ValueClassWithGenericField<T?>?>>() shouldBe arrayNullable
        Avro.schema<List<ValueClassWithGenericField<T>?>>() shouldBe arrayNullable
        Avro.schema<Set<T?>>() shouldBe arrayNullable
        Avro.schema<Set<ValueClassWithGenericField<T>?>>() shouldBe arrayNullable
        Avro.schema<Set<ValueClassWithGenericField<T?>?>>() shouldBe arrayNullable
        Avro.schema<Set<ValueClassWithGenericField<T>?>>() shouldBe arrayNullable
        Avro.schema<Array<T?>>() shouldBe arrayNullable
        Avro.schema<Array<ValueClassWithGenericField<T>?>>() shouldBe arrayNullable
        Avro.schema<Array<ValueClassWithGenericField<T?>?>>() shouldBe arrayNullable
        Avro.schema<Array<ValueClassWithGenericField<T>?>>() shouldBe arrayNullable
        testEncodeDecode(arrayNullable, listOf(ValueClassWithGenericField<T?>(value)), apacheCompatibleValue = listOf(apacheCompatibleValue))
        testEncodeDecode(arrayNullable, listOf(ValueClassWithGenericField<T?>(null)), apacheCompatibleValue = listOf(null))
    }
}

internal inline fun <reified T : Any, reified R : Any> StringSpecRootScope.testSerializationTypeCompatibility(logicalValue: T, encodedAsValue: R) {
    val schema = when {
        encodedAsValue is GenericContainer -> encodedAsValue.schema
        else -> Avro.schema<R>()
    }
    "Support ${logicalValue::class.simpleName} serialization as ${schema.type}" {
        testEncodeDecode(schema, logicalValue, apacheCompatibleValue = encodedAsValue)
    }
    "Support ${logicalValue::class.simpleName} serialization as nullable ${schema.type}" {
        testEncodeDecode(schema.nullable, logicalValue, apacheCompatibleValue = encodedAsValue)
    }
}

inline fun <reified T> testEncodeDecode(
    schema: Schema,
    toEncode: T,
    decoded: Any? = toEncode,
    apacheCompatibleValue: Any? = toEncode,
    serializer: KSerializer<T> = Avro.serializersModule.serializer<T>(),
    expectedBytes: ByteArray = encodeToBytesUsingApacheLib(schema, apacheCompatibleValue),
) {
    Avro.encodeToByteArray(schema, serializer, toEncode) shouldBe expectedBytes
    Avro.decodeFromByteArray(schema, serializer, expectedBytes) shouldBe decoded
}