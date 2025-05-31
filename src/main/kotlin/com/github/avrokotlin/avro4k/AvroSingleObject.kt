package com.github.avrokotlin.avro4k

import com.github.avrokotlin.avro4k.internal.Buffer
import kotlinx.io.Buffer
import kotlinx.io.Sink
import kotlinx.io.Source
import kotlinx.io.asSink
import kotlinx.io.asSource
import kotlinx.io.buffered
import kotlinx.io.readByteArray
import kotlinx.io.readLongLe
import kotlinx.serialization.BinaryFormat
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.serializer
import org.apache.avro.Schema
import org.apache.avro.SchemaNormalization
import java.io.InputStream
import java.io.OutputStream

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

    @Deprecated("Use encodeToSink instead", ReplaceWith("encodeToSink(writerSchema, serializer, value, outputStream.asSink().buffered())"))
    public fun <T> encodeToStream(
        writerSchema: Schema,
        serializer: SerializationStrategy<T>,
        value: T,
        outputStream: OutputStream,
    ): Unit = encodeToSink(writerSchema, serializer, value, outputStream.asSink().buffered())

    public fun <T> encodeToSink(
        writerSchema: Schema,
        serializer: SerializationStrategy<T>,
        value: T,
        sink: Sink,
    ) {
        sink.writeByte(MAGIC_BYTE)
        sink.writeByte(FORMAT_VERSION)
        sink.write(writerSchema.crc64avro())
        avro.encodeToSink(writerSchema, serializer, value, sink)
    }

    @Deprecated("Use decodeFromSource instead", ReplaceWith("decodeFromSource(deserializer, inputStream.asSource().buffered())"))
    public fun <T> decodeFromStream(
        deserializer: DeserializationStrategy<T>,
        inputStream: InputStream,
    ): T = decodeFromSource(deserializer, inputStream.asSource().buffered())

    public fun <T> decodeFromSource(
        deserializer: DeserializationStrategy<T>,
        source: Source,
    ): T {
        check(source.readByte() == MAGIC_BYTE) { "Not a valid single-object avro format, bad magic byte" }
        check(source.readByte() == FORMAT_VERSION) { "Not a valid single-object avro format, bad version byte" }
        val fingerprint = source.readLongLe()
        val writerSchema =
            schemaRegistry(fingerprint) ?: throw SerializationException("schema not found for the given object's schema fingerprint 0x${fingerprint.toString(16)}")

        return avro.decodeFromSource(writerSchema, deserializer, source)
    }

    public override fun <T> decodeFromByteArray(
        deserializer: DeserializationStrategy<T>,
        bytes: ByteArray,
    ): T {
        return decodeFromSource(deserializer, Buffer(bytes))
    }

    override fun <T> encodeToByteArray(
        serializer: SerializationStrategy<T>,
        value: T,
    ): ByteArray {
        val writerSchema = avro.schema(serializer.descriptor)
        return encodeToByteArray(writerSchema, serializer, value)
    }
}

private const val MAGIC_BYTE: Byte = 0xC3.toByte()
private const val FORMAT_VERSION: Byte = 1

public fun <T> AvroSingleObject.encodeToByteArray(
    writerSchema: Schema,
    serializer: SerializationStrategy<T>,
    value: T,
): ByteArray {
    val sink = Buffer()
    encodeToSink(writerSchema, serializer, value, sink)
    return sink.readByteArray()
}

public inline fun <reified T> AvroSingleObject.encodeToByteArray(
    writerSchema: Schema,
    value: T,
): ByteArray = encodeToByteArray(writerSchema, avro.serializersModule.serializer<T>(), value)

public inline fun <reified T> AvroSingleObject.encodeToByteArray(value: T): ByteArray {
    val serializer = avro.serializersModule.serializer<T>()
    return encodeToByteArray(avro.schema(serializer), serializer, value)
}

public inline fun <reified T> AvroSingleObject.decodeFromByteArray(bytes: ByteArray): T = decodeFromByteArray(avro.serializersModule.serializer<T>(), bytes)