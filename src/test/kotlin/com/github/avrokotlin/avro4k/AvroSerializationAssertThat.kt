package com.github.avrokotlin.avro4k

import com.github.avrokotlin.avro4k.io.AvroDecodeFormat
import com.github.avrokotlin.avro4k.io.AvroEncodeFormat
import io.kotest.assertions.Actual
import io.kotest.assertions.Expected
import io.kotest.assertions.failure
import io.kotest.assertions.print.Printed
import io.kotest.assertions.withClue
import io.kotest.matchers.shouldBe
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.SerializersModuleBuilder
import kotlinx.serialization.serializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.nio.file.Path

class AvroSerializationAssertThat<T>(private val valueToEncode: T, private val serializer: KSerializer<T>) {
    private var serializersModule: SerializersModule = Avro.default.serializersModule
    private var config: AvroConfiguration = Avro.default.configuration
    private var readerSchema: Schema = avro.schema(serializer)
    private lateinit var writerSchema: Schema
    private val avro: Avro
        get() = Avro(serializersModule = serializersModule, configuration = config)

    fun withSerializersModule(serializersModuleBuilder: SerializersModuleBuilder.() -> Unit): AvroSerializationAssertThat<T> {
        serializersModule = SerializersModule(serializersModuleBuilder)
        return this
    }

    fun withConfig(config: AvroConfiguration): AvroSerializationAssertThat<T> {
        this.config = config
        return this
    }

    fun generatesSchema(expectedSchema: Schema): AvroSerializationAssertThat<T> {
        avro.schema(serializer) shouldBe expectedSchema
        writerSchema = expectedSchema
        return this
    }

    fun generatesSchema(expectedSchemaResourcePath: Path): AvroSerializationAssertThat<T> {
        return generatesSchema(Schema.Parser().parse(javaClass.getResourceAsStream(expectedSchemaResourcePath.toString())))
    }

    fun isEncodedAs(
        expectedEncodedGenericValue: Any?,
        expectedDecodedValue: T? = valueToEncode
    ): AvroSerializationAssertThat<T> {
        if (!this::writerSchema.isInitialized) {
            throw IllegalStateException("You must call 'shouldGenerateSchema' before calling 'isEncodedAs'.")
        }
        val expectedEncodedBytes = avroApacheEncode(expectedEncodedGenericValue)
        val expectedAvroJson = bytesToAvroJson(expectedEncodedBytes)

        val actualEncodedBytes = avro4kEncode(valueToEncode)
        val actualAvroJson = bytesToAvroJson(actualEncodedBytes)
        withClue("Encoded bytes are not the same as apache avro library.") {
            if (!actualEncodedBytes.contentEquals(expectedEncodedBytes)) {
                throw failure(Expected(Printed(expectedAvroJson)), Actual(Printed(actualAvroJson)))
            }
        }

        val decodedValue = avro4kDecode(expectedEncodedBytes)
        withClue("Decoded value is not the same as the expected one.") {
            decodedValue shouldBe expectedDecodedValue
        }
        return this
    }

    private fun avro4kEncode(value: T): ByteArray {
        val baos = ByteArrayOutputStream()
        avro.openOutputStream(serializer) {
            encodeFormat = AvroEncodeFormat.Binary
            schema = writerSchema
        }.to(baos).write(value).close()
        return baos.toByteArray()
    }

    private fun avro4kDecode(bytes: ByteArray): T {
        return avro.openInputStream(serializer) {
            decodeFormat = AvroDecodeFormat.Binary(this@AvroSerializationAssertThat.writerSchema, this@AvroSerializationAssertThat.readerSchema)
        }.from(bytes).nextOrThrow()
    }

    private fun avroApacheEncode(value: Any?): ByteArray {
        val writer = GenericDatumWriter<Any>(writerSchema)
        val byteArrayOutputStream = ByteArrayOutputStream()
        val encoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null)
        writer.write(convertToAvroGenericValue(value, writerSchema), encoder)
        encoder.flush()
        return byteArrayOutputStream.toByteArray()
    }

    private fun bytesToAvroJson(bytes: ByteArray): String {
        return avroApacheEncodeJson(avroApacheDecode(bytes))
    }

    private fun avroApacheDecode(bytes: ByteArray): Any? {
        val reader = GenericDatumReader<Any>(writerSchema)
        val decoder = DecoderFactory.get().binaryDecoder(ByteArrayInputStream(bytes), null)
        return reader.read(null, decoder)
    }

    private fun avroApacheEncodeJson(value: Any?): String {
        val writer = GenericDatumWriter<Any>(writerSchema)
        val byteArrayOutputStream = ByteArrayOutputStream()
        val encoder = EncoderFactory.get().jsonEncoder(writerSchema, byteArrayOutputStream, true)
        writer.write(convertToAvroGenericValue(value, writerSchema), encoder)
        encoder.flush()
        return byteArrayOutputStream.toByteArray().decodeToString()
    }

    companion object {
        @OptIn(InternalSerializationApi::class)
        inline fun <reified T : Any> assertThat(value: T): AvroSerializationAssertThat<T> {
            return AvroSerializationAssertThat(value, T::class.serializer())
        }
        @Suppress("UNCHECKED_CAST")
        inline fun <reified T : Any> assertThat(value: T, serializer: KSerializer<out T>): AvroSerializationAssertThat<T> {
            return AvroSerializationAssertThat(value, serializer as KSerializer<T>)
        }
    }
}