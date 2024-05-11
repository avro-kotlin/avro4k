package com.github.avrokotlin.avro4k

import io.kotest.assertions.Actual
import io.kotest.assertions.Expected
import io.kotest.assertions.failure
import io.kotest.assertions.print.Printed
import io.kotest.assertions.withClue
import io.kotest.matchers.shouldBe
import kotlinx.serialization.KSerializer
import kotlinx.serialization.serializer
import org.apache.avro.Conversions
import org.apache.avro.Schema
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
    private var avro: Avro = Avro {}

    fun withConfig(builder: AvroBuilder.() -> Unit): AvroEncodingAssertions<T> {
        this.avro = Avro(builderAction = builder)
        return this
    }

    fun isEncodedAs(
        expectedEncodedGenericValue: Any?,
        expectedDecodedValue: T = valueToEncode,
        writerSchema: Schema = avro.schema(serializer),
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
            decodedValue shouldBe expectedDecodedValue
        }
        return this
    }

    inline fun <reified R> isDecodedAs(expected: R) = isDecodedAs(expected, Avro.serializersModule.serializer<R>())

    fun <R> isDecodedAs(
        expected: R,
        serializer: KSerializer<R>,
    ) {
        val writerSchema = avro.schema(this.serializer)
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

internal val Schema.nullable: Schema
    get() = Schema.createUnion(listOf(Schema.create(Schema.Type.NULL), this))