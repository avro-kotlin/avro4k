package com.github.avrokotlin.avro4k

import kotlinx.serialization.BinaryFormat
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.serializer
import org.apache.avro.Schema
import org.apache.avro.SchemaNormalization
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder

/**
 * Single Avro objects are encoded as follows:
 * - A two-byte marker, C3 01, to show that the message is Avro and uses this single-record format (version 1).
 * - The 8-byte little-endian CRC-64-AVRO fingerprint of the object’s schema.
 * - The Avro object encoded using Avro’s binary encoding.
 *
 * [spec](https://avro.apache.org/docs/1.11.1/specification/#single-object-encoding)
 *
 * @param schemaRegistry a function to find a schema by its fingerprint, and returns null when not found. You should use [SchemaNormalization.parsingFingerprint64] to generate the fingerprint.
 */
@ExperimentalSerializationApi
public class AvroSingleObject(
    private val schemaRegistry: (fingerprint: Long) -> Schema?,
    @PublishedApi
    internal val avro: Avro = Avro,
) : BinaryFormat {
    override val serializersModule: SerializersModule
        get() = avro.serializersModule

    private fun Schema.crc64avro(): ByteArray = SchemaNormalization.parsingFingerprint("CRC-64-AVRO", this)

    public fun <T> encodeToStream(
        writerSchema: Schema,
        serializer: SerializationStrategy<T>,
        value: T,
        outputStream: OutputStream,
    ) {
        outputStream.write(MAGIC_BYTE)
        outputStream.write(FORMAT_VERSION)
        outputStream.write(writerSchema.crc64avro())
        avro.encodeToStream(writerSchema, serializer, value, outputStream)
    }

    public fun <T> decodeFromStream(
        deserializer: DeserializationStrategy<T>,
        inputStream: InputStream,
    ): T {
        check(inputStream.read() == MAGIC_BYTE) { "Not a valid single-object avro format, bad magic byte" }
        check(inputStream.read() == FORMAT_VERSION) { "Not a valid single-object avro format, bad version byte" }
        val fingerprint = ByteBuffer.wrap(ByteArray(8).apply { inputStream.read(this) }).order(ByteOrder.LITTLE_ENDIAN).getLong()
        val writerSchema =
            schemaRegistry(fingerprint) ?: throw SerializationException("schema not found for the given object's schema fingerprint 0x${fingerprint.toString(16)}")

        return avro.decodeFromStream(writerSchema, deserializer, inputStream)
    }

    public override fun <T> decodeFromByteArray(
        deserializer: DeserializationStrategy<T>,
        bytes: ByteArray,
    ): T {
        return bytes.inputStream().use {
            decodeFromStream(deserializer, it)
        }
    }

    override fun <T> encodeToByteArray(
        serializer: SerializationStrategy<T>,
        value: T,
    ): ByteArray {
        return encodeToByteArray(avro.schema(serializer.descriptor), serializer, value)
    }
}

private const val MAGIC_BYTE: Int = 0xC3
private const val FORMAT_VERSION: Int = 1

public fun <T> AvroSingleObject.encodeToByteArray(
    writerSchema: Schema,
    serializer: SerializationStrategy<T>,
    value: T,
): ByteArray =
    ByteArrayOutputStream().apply {
        encodeToStream(writerSchema, serializer, value, this)
    }.toByteArray()

public inline fun <reified T> AvroSingleObject.encodeToByteArray(
    writerSchema: Schema,
    value: T,
): ByteArray = encodeToByteArray(writerSchema, avro.serializersModule.serializer<T>(), value)

public inline fun <reified T> AvroSingleObject.encodeToByteArray(value: T): ByteArray {
    val serializer = avro.serializersModule.serializer<T>()
    return encodeToByteArray(avro.schema(serializer), serializer, value)
}

public inline fun <reified T> AvroSingleObject.decodeFromByteArray(bytes: ByteArray): T = decodeFromByteArray(avro.serializersModule.serializer<T>(), bytes)