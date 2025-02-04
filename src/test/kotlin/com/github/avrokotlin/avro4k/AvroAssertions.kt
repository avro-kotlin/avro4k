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

internal inline fun <reified T : Any> StringSpecRootScope.basicScalarEncodeDecodeTests(
    value: T,
    expectedSchema: Schema,
    apacheCompatibleValue: Any? = value,
) {
    "support runtime ${expectedSchema.type} type ${value::class.qualifiedName} serialization" {
        Avro.schema<T>() shouldBe expectedSchema
        testEncodeDecode(expectedSchema, value, apacheCompatibleValue = apacheCompatibleValue)

        Avro.schema<ValueClassWithGenericField<T>>() shouldBe expectedSchema
        testEncodeDecode(expectedSchema, ValueClassWithGenericField(value), apacheCompatibleValue = apacheCompatibleValue)
    }
    "support runtime ${expectedSchema.type} type ${value::class.qualifiedName} serialization as nullable" {
        Avro.schema<T?>() shouldBe expectedSchema.nullable
        testEncodeDecode<T?>(expectedSchema.nullable, value, apacheCompatibleValue = apacheCompatibleValue)
        testEncodeDecode<T?>(expectedSchema.nullable, null)

        Avro.schema<ValueClassWithGenericField<T?>>() shouldBe expectedSchema.nullable
        testEncodeDecode(expectedSchema.nullable, ValueClassWithGenericField<T?>(value), apacheCompatibleValue = apacheCompatibleValue)
        testEncodeDecode(expectedSchema.nullable, ValueClassWithGenericField<T?>(null), apacheCompatibleValue = null)

        Avro.schema<ValueClassWithGenericField<T?>?>() shouldBe expectedSchema.nullable
        testEncodeDecode<ValueClassWithGenericField<T?>?>(expectedSchema.nullable, null)

        Avro.schema<ValueClassWithGenericField<T>?>() shouldBe expectedSchema.nullable
        testEncodeDecode<ValueClassWithGenericField<T>?>(expectedSchema.nullable, null)
    }
    "support runtime ${expectedSchema.type} type ${value::class.qualifiedName} in record" {
        val record =
            SchemaBuilder.record("RecordWithGenericField").fields()
                .name("field").type(expectedSchema).noDefault()
                .endRecord()
        Avro.schema<RecordWithGenericField<T>>() shouldBe record
        Avro.schema<RecordWithGenericField<ValueClassWithGenericField<T>>>() shouldBe record
        testEncodeDecode(
            record,
            RecordWithGenericField(value),
            apacheCompatibleValue = GenericData.Record(record).also { it.put(0, apacheCompatibleValue) }
        )
        testEncodeDecode(
            record,
            RecordWithGenericField(ValueClassWithGenericField(value)),
            apacheCompatibleValue = GenericData.Record(record).also { it.put(0, apacheCompatibleValue) }
        )
    }
    "support runtime ${expectedSchema.type} type ${value::class.qualifiedName} in record as nullable field" {
        val expectedRecordSchemaNullable =
            SchemaBuilder.record("RecordWithGenericField").fields()
                .name("field").type(expectedSchema.nullable).withDefault(null)
                .endRecord()
        Avro.schema<RecordWithGenericField<T?>>() shouldBe expectedRecordSchemaNullable
        Avro.schema<RecordWithGenericField<ValueClassWithGenericField<T?>>>() shouldBe expectedRecordSchemaNullable
        Avro.schema<RecordWithGenericField<ValueClassWithGenericField<T?>?>>() shouldBe expectedRecordSchemaNullable
        Avro.schema<RecordWithGenericField<ValueClassWithGenericField<T>?>>() shouldBe expectedRecordSchemaNullable

        val recordNullable =
            SchemaBuilder.record("RecordWithGenericField").fields()
                .name("field").type(expectedSchema.nullable).noDefault()
                .endRecord()
        testEncodeDecode(
            recordNullable,
            RecordWithGenericField<T?>(value),
            apacheCompatibleValue = GenericData.Record(recordNullable).also { it.put(0, apacheCompatibleValue) }
        )
        testEncodeDecode(
            recordNullable,
            RecordWithGenericField<T?>(null),
            apacheCompatibleValue = GenericData.Record(recordNullable).also { it.put(0, null) }
        )
        testEncodeDecode(
            recordNullable,
            RecordWithGenericField(ValueClassWithGenericField<T?>(value)),
            apacheCompatibleValue = GenericData.Record(recordNullable).also { it.put(0, apacheCompatibleValue) }
        )
        testEncodeDecode(
            recordNullable,
            RecordWithGenericField(ValueClassWithGenericField<T?>(null)),
            apacheCompatibleValue = GenericData.Record(recordNullable).also { it.put(0, null) }
        )
    }
    "support runtime ${expectedSchema.type} type ${value::class.qualifiedName} in map" {
        val map = SchemaBuilder.map().values(expectedSchema)
        Avro.schema<Map<String, T>>() shouldBe map
        Avro.schema<Map<String, ValueClassWithGenericField<T>>>() shouldBe map
        Avro.schema<Map<T, ValueClassWithGenericField<T>>>() shouldBe map
        Avro.schema<Map<ValueClassWithGenericField<T>, ValueClassWithGenericField<T>>>() shouldBe map
        Avro.schema<Map<T, T>>() shouldBe map
        Avro.schema<Map<ValueClassWithGenericField<T>, T>>() shouldBe map
        testEncodeDecode(map, mapOf("key" to value), apacheCompatibleValue = mapOf("key" to apacheCompatibleValue))
        testEncodeDecode(map, mapOf("key" to ValueClassWithGenericField(value)), apacheCompatibleValue = mapOf("key" to apacheCompatibleValue))

        val mapNullable = SchemaBuilder.map().values(expectedSchema.nullable)
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
    "support runtime ${expectedSchema.type} type ${value::class.qualifiedName} in array" {
        val array = SchemaBuilder.array().items(expectedSchema)
        Avro.schema<List<T>>() shouldBe array
        Avro.schema<List<ValueClassWithGenericField<T>>>() shouldBe array
        Avro.schema<Set<T>>() shouldBe array
        Avro.schema<Set<ValueClassWithGenericField<T>>>() shouldBe array
        Avro.schema<Array<T>>() shouldBe array
        Avro.schema<Array<ValueClassWithGenericField<T>>>() shouldBe array
        testEncodeDecode(array, listOf(value), apacheCompatibleValue = listOf(apacheCompatibleValue))
        testEncodeDecode(array, listOf(ValueClassWithGenericField(value)), apacheCompatibleValue = listOf(apacheCompatibleValue))

        val arrayNullable = SchemaBuilder.array().items(expectedSchema.nullable)
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

internal inline fun <reified T : Any, reified R : Any> StringSpecRootScope.testSerializationTypeCompatibility(
    logicalValue: T,
    apacheCompatibleValue: R,
    writerSchema: Schema,
) {
    val originalSchema = Avro.schema<T>()
    "support coercion from ${originalSchema.type} runtime type ${logicalValue::class.qualifiedName} to type ${writerSchema.type}" {
        testEncodeDecode(writerSchema, logicalValue, apacheCompatibleValue = apacheCompatibleValue)
        testEncodeDecode(writerSchema, ValueClassWithGenericField(logicalValue), apacheCompatibleValue = apacheCompatibleValue)
    }
    "support coercion from ${originalSchema.type} runtime type ${logicalValue::class.qualifiedName} to nullable type ${writerSchema.type}" {
        testEncodeDecode<T?>(writerSchema.nullable, logicalValue, apacheCompatibleValue = apacheCompatibleValue)
        testEncodeDecode<T?>(writerSchema.nullable, null)

        testEncodeDecode(writerSchema.nullable, ValueClassWithGenericField<T?>(logicalValue), apacheCompatibleValue = apacheCompatibleValue)
        testEncodeDecode(writerSchema.nullable, ValueClassWithGenericField<T?>(null), apacheCompatibleValue = null)

        testEncodeDecode<ValueClassWithGenericField<T?>?>(writerSchema.nullable, null)
        testEncodeDecode<ValueClassWithGenericField<T>?>(writerSchema.nullable, null)
    }
    "support coercion from ${originalSchema.type} runtime type ${logicalValue::class.qualifiedName} inside a record's field with type ${writerSchema.type}" {
        val record =
            SchemaBuilder.record("RecordWithGenericField").fields()
                .name("field").type(writerSchema).noDefault()
                .endRecord()
        testEncodeDecode(
            record,
            RecordWithGenericField(logicalValue),
            apacheCompatibleValue = GenericData.Record(record).also { it.put(0, apacheCompatibleValue) }
        )
        testEncodeDecode(
            record,
            RecordWithGenericField(ValueClassWithGenericField(logicalValue)),
            apacheCompatibleValue = GenericData.Record(record).also { it.put(0, apacheCompatibleValue) }
        )
    }
    "support coercion from ${originalSchema.type} runtime type ${logicalValue::class.qualifiedName} inside a record's field with nullable type ${writerSchema.type}" {
        val recordNullable =
            SchemaBuilder.record("RecordWithGenericField").fields()
                .name("field").type(writerSchema.nullable).noDefault()
                .endRecord()
        testEncodeDecode(
            recordNullable,
            RecordWithGenericField<T?>(logicalValue),
            apacheCompatibleValue = GenericData.Record(recordNullable).also { it.put(0, apacheCompatibleValue) }
        )
        testEncodeDecode(
            recordNullable,
            RecordWithGenericField<T?>(null),
            apacheCompatibleValue = GenericData.Record(recordNullable).also { it.put(0, null) }
        )
        testEncodeDecode(
            recordNullable,
            RecordWithGenericField(ValueClassWithGenericField<T?>(logicalValue)),
            apacheCompatibleValue = GenericData.Record(recordNullable).also { it.put(0, apacheCompatibleValue) }
        )
        testEncodeDecode(
            recordNullable,
            RecordWithGenericField(ValueClassWithGenericField<T?>(null)),
            apacheCompatibleValue = GenericData.Record(recordNullable).also { it.put(0, null) }
        )
    }
    "support coercion from ${originalSchema.type} runtime type ${logicalValue::class.qualifiedName} inside a map of ${writerSchema.type} values" {
        val map = SchemaBuilder.map().values(writerSchema)
        testEncodeDecode(map, mapOf("key" to logicalValue), apacheCompatibleValue = mapOf("key" to apacheCompatibleValue))
        testEncodeDecode(map, mapOf("key" to ValueClassWithGenericField(logicalValue)), apacheCompatibleValue = mapOf("key" to apacheCompatibleValue))

        val mapNullable = SchemaBuilder.map().values(writerSchema.nullable)
        testEncodeDecode(mapNullable, mapOf("key" to ValueClassWithGenericField<T?>(logicalValue)), apacheCompatibleValue = mapOf("key" to apacheCompatibleValue))
        testEncodeDecode(mapNullable, mapOf("key" to ValueClassWithGenericField<T?>(null)), apacheCompatibleValue = mapOf("key" to null))
    }
    "support coercion from ${originalSchema.type} runtime type ${logicalValue::class.qualifiedName} inside an array of ${writerSchema.type} items" {
        val array = SchemaBuilder.array().items(writerSchema)
        testEncodeDecode(array, listOf(logicalValue), apacheCompatibleValue = listOf(apacheCompatibleValue))
        testEncodeDecode(array, listOf(ValueClassWithGenericField(logicalValue)), apacheCompatibleValue = listOf(apacheCompatibleValue))

        val arrayNullable = SchemaBuilder.array().items(writerSchema.nullable)
        testEncodeDecode(arrayNullable, listOf(ValueClassWithGenericField<T?>(logicalValue)), apacheCompatibleValue = listOf(apacheCompatibleValue))
        testEncodeDecode(arrayNullable, listOf(ValueClassWithGenericField<T?>(null)), apacheCompatibleValue = listOf(null))
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
    val decodedValue = Avro.decodeFromByteArray(schema, serializer, expectedBytes) as Any?
    try {
        decodedValue shouldBe decoded
    } catch (originalError: Throwable) {
        if (!deepEquals(decodedValue, decoded)) {
            throw originalError
        }
    }
}

/**
 * kotest doesn't handle deep equals for value classes, so we need to implement it ourselves.
 */
fun deepEquals(
    a: Any?,
    b: Any?,
): Boolean {
    if (a === b) return true
    if (a == b) return true
    if (a == null || b == null) return false
    if (a is ByteArray && b is ByteArray) return a.contentEquals(b)
    if (a is ValueClassWithGenericField<*> && b is ValueClassWithGenericField<*>) return deepEquals(a.value, b.value)
    if (a is Collection<*> && b is Collection<*>) {
        if (a.size != b.size) return false
        return a.zip(b).all { (a, b) -> deepEquals(a, b) }
    }
    if (a is Map<*, *> && b is Map<*, *>) {
        if (a.size != b.size) return false
        return a.all { (key, value) -> deepEquals(value, b[key]) }
    }
    if (a::class.isValue && b::class.isValue) {
        return deepEquals(unboxValueClass(a), unboxValueClass(b))
    }
    return a == b
}

private fun unboxValueClass(value: Any): Any? {
    return value::class.java.getDeclaredMethod("unbox-impl").apply { isAccessible = true }.invoke(value)
}