package com.github.avrokotlin.avro4k.endecode

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.RecordBuilderForTest
import io.kotest.assertions.fail
import io.kotest.assertions.withClue
import io.kotest.core.factory.TestFactory
import io.kotest.core.spec.DslDrivenSpec
import io.kotest.matchers.equality.shouldBeEqualToComparingFields
import io.kotest.matchers.shouldBe
import io.kotest.mpp.newInstanceNoArgConstructorOrObjectInstance
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.serializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.Decoder
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.Encoder
import org.apache.avro.io.EncoderFactory
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream

sealed interface EnDecoder {
    val name: String
    var avro: Avro

    fun encodeGenericRecordForComparison(
        value: GenericRecord,
        schema: Schema,
    ): ByteArray

    fun <T> decode(
        byteArray: ByteArray,
        deserializer: DeserializationStrategy<T>,
        readSchema: Schema,
        writeSchema: Schema,
    ): T

    fun <T> encode(
        value: T,
        serializer: SerializationStrategy<T>,
        schema: Schema,
    ): ByteArray
}

class AvroLibEnDecoder : EnDecoder {
    override var avro: Avro = Avro.default
    override val name: String = "AvroLibrary"

    override fun <T> encode(
        value: T,
        serializer: SerializationStrategy<T>,
        schema: Schema,
    ): ByteArray {
        val asRecord = avro.toRecord(serializer, schema, value)
        return encodeGenericRecordForComparison(asRecord, schema)
    }

    override fun encodeGenericRecordForComparison(
        value: GenericRecord,
        schema: Schema,
    ): ByteArray {
        val writer = GenericDatumWriter<GenericRecord>(schema)
        val byteArrayOutputStream = ByteArrayOutputStream()
        val encoder = avroLibEncoder(schema, byteArrayOutputStream)
        writer.write(value, encoder)
        encoder.flush()
        return byteArrayOutputStream.toByteArray()
    }

    override fun <T> decode(
        byteArray: ByteArray,
        deserializer: DeserializationStrategy<T>,
        readSchema: Schema,
        writeSchema: Schema,
    ): T {
        val input = ByteArrayInputStream(byteArray)
        val reader = GenericDatumReader<GenericRecord>(writeSchema, readSchema)
        val genericData = reader.read(null, avroLibDecoder(writeSchema, input))
        return avro.fromRecord(deserializer, genericData)
    }

    fun avroLibEncoder(
        schema: Schema,
        outputStream: ByteArrayOutputStream,
    ): Encoder = EncoderFactory.get().jsonEncoder(schema, outputStream)

    fun avroLibDecoder(
        schema: Schema,
        inputStream: InputStream,
    ): Decoder = DecoderFactory.get().jsonDecoder(schema, inputStream)
}

inline fun <reified T> EnDecoder.testEncodeDecode(
    value: T,
    shouldMatch: RecordBuilderForTest,
    serializer: KSerializer<T> = avro.serializersModule.serializer<T>(),
    schema: Schema = avro.schema(serializer),
) {
    val encoded = testEncodeIsEqual(value, shouldMatch, serializer, schema)
    testDecodeIsEqual(encoded, value, serializer, schema)
}

inline fun <reified T> EnDecoder.testEncodeIsEqual(
    value: T,
    shouldMatch: RecordBuilderForTest,
    serializer: SerializationStrategy<T> = avro.serializersModule.serializer<T>(),
    schema: Schema = avro.schema(serializer),
): ByteArray {
    val record = shouldMatch.createRecord(schema)
    val encodedValue = encode(value, serializer, schema)
    withClue("Encoded result was not equal to the encoded result of the apache avro library.") {
        encodedValue shouldBe encodeGenericRecordForComparison(record, schema)
    }
    return encodedValue
}

inline fun <reified T> EnDecoder.testDecodeIsEqual(
    byteArray: ByteArray,
    value: T,
    serializer: KSerializer<T> = avro.serializersModule.serializer<T>(),
    readSchema: Schema = avro.schema(serializer),
    writeSchema: Schema = readSchema,
): T {
    val decodedValue = decode(byteArray, serializer, readSchema, writeSchema)
    withClue("Decoded result was not equal to the passed value.") {
        if (decodedValue == null && value != null) {
            fail("Decoded value is null but '$value' is expected.")
        } else if (decodedValue != null && value != null) {
            decodedValue shouldBeEqualToComparingFields value
        }
    }
    return decodedValue
}

fun DslDrivenSpec.includeForEveryEncoder(createFactoryToInclude: (EnDecoder) -> TestFactory) {
    EnDecoder::class.sealedSubclasses.map { it.newInstanceNoArgConstructorOrObjectInstance() }.forEach {
        include(it.name, createFactoryToInclude.invoke(it))
    }
}