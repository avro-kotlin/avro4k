package com.github.avrokotlin.avro4k

import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.SerializationException
import kotlinx.serialization.SerializationStrategy
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
 * @param findSchemaByFingerprint a function to find a schema by its fingerprint, and returns null when not found
 */
class AvroSingleObject(
    private val findSchemaByFingerprint: (Long) -> Schema?,
    val avro: Avro = Avro,
) {
    private fun Schema.crc64avro(): ByteArray = SchemaNormalization.parsingFingerprint("CRC-64-AVRO", this)

    fun <T> encodeToStream(
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

    fun <T> decodeFromStream(
        deserializer: DeserializationStrategy<T>,
        inputStream: InputStream,
    ): T {
        check(inputStream.read() == MAGIC_BYTE) { "Not a valid single-object avro format, bad magic byte" }
        check(inputStream.read() == FORMAT_VERSION) { "Not a valid single-object avro format, bad version byte" }
        val fingerprint = ByteBuffer.wrap(ByteArray(8).apply { inputStream.read(this) }).order(ByteOrder.LITTLE_ENDIAN).getLong()
        val writerSchema =
            findSchemaByFingerprint(fingerprint) ?: throw SerializationException("schema not found for the given object's schema fingerprint 0x${fingerprint.toString(16)}")

        return avro.decodeFromStream(writerSchema, deserializer, inputStream)
    }
}

private const val MAGIC_BYTE: Int = 0xC3
private const val FORMAT_VERSION: Int = 1

fun <T> AvroSingleObject.encodeToByteArray(
    writerSchema: Schema,
    serializer: SerializationStrategy<T>,
    value: T,
): ByteArray =
    ByteArrayOutputStream().apply {
        encodeToStream(writerSchema, serializer, value, this)
    }.toByteArray()

inline fun <reified T> AvroSingleObject.encodeToByteArray(
    writerSchema: Schema,
    value: T,
): ByteArray = encodeToByteArray(writerSchema, avro.serializersModule.serializer<T>(), value)

inline fun <reified T> AvroSingleObject.encodeToByteArray(value: T): ByteArray {
    val serializer = avro.serializersModule.serializer<T>()
    return encodeToByteArray(avro.schema(serializer), serializer, value)
}

fun <T> AvroSingleObject.decodeFromByteArray(
    deserializer: DeserializationStrategy<T>,
    bytes: ByteArray,
): T =
    bytes.inputStream().use {
        decodeFromStream(deserializer, it)
    }

inline fun <reified T> AvroSingleObject.decodeFromByteArray(bytes: ByteArray): T = decodeFromByteArray(avro.serializersModule.serializer<T>(), bytes)