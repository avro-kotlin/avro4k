package com.github.avrokotlin.avro4k

import com.github.avrokotlin.avro4k.AvroSerializationDelegate.Builtins.DataFile
import com.github.avrokotlin.avro4k.AvroSerializationDelegate.Builtins.Pure
import com.github.avrokotlin.avro4k.AvroSerializationDelegate.Builtins.SingleObject
import com.github.avrokotlin.avro4k.schema.NamingStrategy
import com.github.avrokotlin.avro4k.schema.schemaFor
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.modules.EmptySerializersModule
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.serializer
import org.apache.avro.Schema
import org.apache.avro.SchemaNormalization
import org.apache.avro.file.DataFileStream
import org.apache.avro.file.DataFileWriter
import org.apache.avro.io.DatumReader
import org.apache.avro.io.DatumWriter
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder
import org.apache.avro.io.Decoder as AvroDecoder
import org.apache.avro.io.Encoder as AvroEncoder


/**
 * The goal of this class is to serialize and deserialize in avro binary format, not in GenericRecords.
 */
sealed class Avro2(
    val configuration: AvroConfiguration,
    val serializersModule: SerializersModule,
) {
    companion object Default : Avro2(
        AvroConfiguration(),
        EmptySerializersModule() // todo maybe add GenericRecord and other generic stuff serializers
    )

    @OptIn(ExperimentalSerializationApi::class)
    fun <T> schema(serializer: KSerializer<T>): Schema {
        val descriptor = serializer.descriptor
        return schemaFor(
            serializersModule,
            descriptor,
            descriptor.annotations,
            configuration,
            mutableMapOf()
        ).schema()
    }

    fun <T> encodeToStream(writerSchema: Schema, serializer: SerializationStrategy<T>, value: T, outputStream: OutputStream) {
        configuration.serializationDelegate.encodeValue(this, writerSchema, serializer, value, outputStream)
    }

    fun <T> encodeSequenceToStream(writerSchema: Schema, serializer: SerializationStrategy<T>, sequence: Sequence<T>, outputStream: OutputStream) {
        configuration.serializationDelegate.encodeSequence(this, writerSchema, serializer, sequence, outputStream)
    }

    fun <T> decodeFromStream(readerSchema: Schema, deserializer: DeserializationStrategy<T>, inputStream: InputStream): T {
        return configuration.serializationDelegate.decodeValue(this, readerSchema, deserializer, inputStream)
    }

    fun <T> decodeSequenceFromStream(readerSchema: Schema, deserializer: DeserializationStrategy<T>, inputStream: InputStream): Sequence<T> {
        return configuration.serializationDelegate.decodeSequence(this, readerSchema, deserializer, inputStream)
    }
}

fun Avro2(from: Avro2 = Avro2, builderAction: Avro2Builder.() -> Unit): Avro2 {
    val builder = Avro2Builder(from)
    builder.builderAction()
    return Avro2Impl(builder.build(), builder.serializersModule)
}

class Avro2Builder internal constructor(Avro2: Avro2) {
    var namingStrategy: NamingStrategy = Avro2.configuration.namingStrategy
    var implicitNulls: Boolean = Avro2.configuration.implicitNulls
    var serializationDelegate: AvroSerializationDelegate = Avro2.configuration.serializationDelegate
    var serializersModule: SerializersModule = Avro2.serializersModule

    fun build() = AvroConfiguration(
        namingStrategy = this.namingStrategy,
        implicitNulls = this.implicitNulls,
        serializationDelegate = this.serializationDelegate,
    )
}

private class Avro2Impl(configuration: AvroConfiguration, serializersModule: SerializersModule) :
    Avro2(configuration, serializersModule)

// schema gen extensions

inline fun <reified T> Avro2.schema(): Schema {
    val serializer = serializersModule.serializer<T>()
    return schema(serializer)
}

// encoding extensions

fun <T> Avro2.encodeToByteArray(writerSchema: Schema, serializer: SerializationStrategy<T>, value: T): ByteArray {
    val outputStream = ByteArrayOutputStream()
    encodeToStream(writerSchema, serializer, value, outputStream)
    return outputStream.toByteArray()
}

inline fun <reified T> Avro2.encodeToByteArray(value: T): ByteArray {
    val serializer = serializersModule.serializer<T>()
    return encodeToByteArray(schema(serializer), serializer, value)
}

inline fun <reified T> Avro2.encodeToStream(value: T, outputStream: OutputStream) {
    val serializer = serializersModule.serializer<T>()
    encodeToStream(schema(serializer), serializer, value, outputStream)
}

fun <T> Avro2.encodeSequenceToByteArray(writerSchema: Schema, serializer: SerializationStrategy<T>, sequence: Sequence<T>): ByteArray {
    val outputStream = ByteArrayOutputStream()
    encodeSequenceToStream(writerSchema, serializer, sequence, outputStream)
    return outputStream.toByteArray()
}

inline fun <reified T> Avro2.encodeSequenceToByteArray(sequence: Sequence<T>): ByteArray {
    val serializer = serializersModule.serializer<T>()
    return encodeSequenceToByteArray(schema(serializer), serializer, sequence)
}

inline fun <reified T> Avro2.encodeSequenceToStream(sequence: Sequence<T>, outputStream: OutputStream) {
    val serializer = serializersModule.serializer<T>()
    encodeSequenceToStream(schema(serializer), serializer, sequence, outputStream)
}

// decoding extensions

fun <T> Avro2.decodeFromByteArray(readerSchema: Schema, deserializer: DeserializationStrategy<T>, bytes: ByteArray): T {
    return decodeFromStream(readerSchema, deserializer, ByteArrayInputStream(bytes))
}

inline fun <reified T> Avro2.decodeFromStream(inputStream: InputStream): T {
    val serializer = serializersModule.serializer<T>()
    return decodeFromStream(schema(serializer), serializer, inputStream)
}

inline fun <reified T> Avro2.decodeFromByteArray(bytes: ByteArray): T {
    val serializer = serializersModule.serializer<T>()
    return decodeFromByteArray(schema(serializer), serializer, bytes)
}

fun <T> Avro2.decodeSequenceFromByteArray(readerSchema: Schema, deserializer: DeserializationStrategy<T>, bytes: ByteArray): Sequence<T> {
    return decodeSequenceFromStream(readerSchema, deserializer, ByteArrayInputStream(bytes))
}

inline fun <reified T> Avro2.decodeSequenceFromStream(inputStream: InputStream): Sequence<T> {
    val serializer = serializersModule.serializer<T>()
    return decodeSequenceFromStream(schema(serializer), serializer, inputStream)
}

inline fun <reified T> Avro2.decodeSequenceFromByteArray(bytes: ByteArray): Sequence<T> {
    val serializer = serializersModule.serializer<T>()
    return decodeSequenceFromByteArray(schema(serializer), serializer, bytes)
}


/**
 * A serialization delegate for avro helps to intercept encoding and decoding to allowing customising formats.
 *
 * Here the available official delegates:
 * - [Pure]
 * - [DataFile]
 * - [SingleObject]
 *
 * If you need something custom, you can extend [CustomAvroSerializationDelegate].
 */
sealed class AvroSerializationDelegate {
    abstract val encodedAs: EncodedAs

    /**
     * The created encoder is stateful, so you can use this encoder for multiple encodings.
     */
    fun <T> newEncoder(avro: Avro2, writerSchema: Schema, serializer: SerializationStrategy<T>, outputStream: OutputStream): Encoder {
        val avroEncoder = when (encodedAs) {
            EncodedAs.BINARY -> EncoderFactory.get().binaryEncoder(outputStream, null)
            EncodedAs.JSON_COMPACT -> EncoderFactory.get().jsonEncoder(writerSchema, outputStream, false)
            EncodedAs.JSON_PRETTY -> EncoderFactory.get().jsonEncoder(writerSchema, outputStream, true)
        }
        return newEncoder(avro, writerSchema, serializer, avroEncoder)
    }

    /**
     * The created decoder is stateful, so you can use this decoder for multiple decodings.
     */
    fun <T> newDecoder(avro: Avro2, readerSchema: Schema, writerSchema: Schema, deserializer: DeserializationStrategy<T>, inputStream: InputStream): Decoder {
        val avroDecoder = when (encodedAs) {
            EncodedAs.BINARY -> DecoderFactory.get().binaryDecoder(inputStream, null)
            EncodedAs.JSON_COMPACT, EncodedAs.JSON_PRETTY -> DecoderFactory.get().jsonDecoder(readerSchema, inputStream)
        }
        return newDecoder(avro, readerSchema, writerSchema, deserializer, avroDecoder)
    }

    internal fun <T> newEncoder(avro: Avro2, writerSchema: Schema, serializer: SerializationStrategy<T>, avroEncoder: AvroEncoder): Encoder {
        TODO()
    }

    internal fun <T> newDecoder(avro: Avro2, readerSchema: Schema, writerSchema: Schema, deserializer: DeserializationStrategy<T>, avroDecoder: AvroDecoder): Decoder {
        TODO()
    }

    abstract fun <T> encodeValue(avro: Avro2, writerSchema: Schema, serializer: SerializationStrategy<T>, value: T, outputStream: OutputStream)
    abstract fun <T> encodeSequence(avro: Avro2, writerSchema: Schema, serializer: SerializationStrategy<T>, sequence: Sequence<T>, outputStream: OutputStream)
    abstract fun <T> decodeValue(avro: Avro2, readerSchema: Schema, deserializer: DeserializationStrategy<T>, inputStream: InputStream): T
    abstract fun <T> decodeSequence(avro: Avro2, readerSchema: Schema, deserializer: DeserializationStrategy<T>, inputStream: InputStream): Sequence<T>

    enum class EncodedAs {
        BINARY, JSON_COMPACT, JSON_PRETTY
    }

    companion object Builtins {

        /**
         * Encode and decode binary values without anything before like metadata or schema.
         * Use it when you exactly know the schema used when decoding.
         * [decodeValue] will not check if there is remaining bytes in the input stream, while [decodeSequence] will loop until the end of the stream.
         */
        data class Pure(override val encodedAs: EncodedAs) : AvroSerializationDelegate() {
            override fun <T> encodeValue(avro: Avro2, writerSchema: Schema, serializer: SerializationStrategy<T>, value: T, outputStream: OutputStream) {
                newEncoder(avro, writerSchema, serializer, outputStream)
                    .encodeSerializableValue(serializer, value)
            }

            override fun <T> encodeSequence(avro: Avro2, writerSchema: Schema, serializer: SerializationStrategy<T>, sequence: Sequence<T>, outputStream: OutputStream) {
                val encoder = newEncoder(avro, writerSchema, serializer, outputStream)
                sequence.forEach {
                    encoder.encodeSerializableValue(serializer, it)
                }
            }

            override fun <T> decodeValue(avro: Avro2, readerSchema: Schema, deserializer: DeserializationStrategy<T>, inputStream: InputStream): T =
                newDecoder(avro, readerSchema, readerSchema, deserializer, inputStream)
                    .decodeSerializableValue(deserializer)

            override fun <T> decodeSequence(avro: Avro2, readerSchema: Schema, deserializer: DeserializationStrategy<T>, inputStream: InputStream): Sequence<T> =
                sequence<T> {
                    val decoder = newDecoder(avro, readerSchema, readerSchema, deserializer, inputStream)
                    while (inputStream.available() > 0) {
                        yield(decoder.decodeSerializableValue(deserializer))
                    }
                }.constrainOnce()
        }

        /**
         * Encode and decode values in object container files, also known as avro data file format.
         *
         * [spec](https://avro.apache.org/docs/1.11.1/specification/#object-container-files)
         */
        object DataFile : AvroSerializationDelegate() {
            override val encodedAs: EncodedAs
                get() = EncodedAs.BINARY

            override fun <T> encodeValue(avro: Avro2, writerSchema: Schema, serializer: SerializationStrategy<T>, value: T, outputStream: OutputStream): Unit =
                encodeSequence(avro, writerSchema, serializer, sequenceOf(value), outputStream)

            override fun <T> decodeValue(avro: Avro2, readerSchema: Schema, deserializer: DeserializationStrategy<T>, inputStream: InputStream): T =
                decodeSequence(avro, readerSchema, deserializer, inputStream).single()

            override fun <T> encodeSequence(avro: Avro2, writerSchema: Schema, serializer: SerializationStrategy<T>, sequence: Sequence<T>, outputStream: OutputStream) {
                val datumWriter: DatumWriter<T> = KotlinxSerializationDatumWriter(serializer, avro)
                DataFileWriter(datumWriter).create(writerSchema, outputStream).use { dataFileWriter ->
                    sequence.forEach {
                        dataFileWriter.append(it)
                    }
                }
            }

            internal class KotlinxSerializationDatumWriter<T>(
                private val serializer: SerializationStrategy<T>,
                private val avro: Avro2,
            ) : DatumWriter<T> {
                private lateinit var writerSchema: Schema

                override fun setSchema(schema: Schema) {
                    writerSchema = schema
                }

                override fun write(datum: T, encoder: AvroEncoder) {
                    newEncoder(avro, writerSchema, serializer, encoder)
                        .encodeSerializableValue(serializer, datum)
                }
            }

            override fun <T> decodeSequence(avro: Avro2, readerSchema: Schema, deserializer: DeserializationStrategy<T>, inputStream: InputStream): Sequence<T> =
                sequence {
                    val datumReader: DatumReader<T> = KotlinxSerializationDatumReader(deserializer, avro, readerSchema)
                    DataFileStream(inputStream, datumReader).use { dataFileStream ->
                        yieldAll(dataFileStream.iterator())
                    }
                }.constrainOnce()

            internal class KotlinxSerializationDatumReader<T>(
                private val deserializer: DeserializationStrategy<T>,
                private val avro: Avro2,
                private val readerSchema: Schema,
            ) : DatumReader<T> {
                private lateinit var writerSchema: Schema

                override fun setSchema(schema: Schema) {
                    writerSchema = schema
                }

                override fun read(reuse: T?, decoder: AvroDecoder): T {
                    return newDecoder(avro, readerSchema, writerSchema, deserializer, decoder)
                        .decodeSerializableValue(deserializer)
                }
            }
        }

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
        class SingleObject(private val findSchemaByFingerprint: (Long) -> Schema?) : AvroSerializationDelegate() {
            companion object {
                private const val MAGIC_BYTE: Int = 0xC3
                private const val FORMAT_VERSION: Int = 1
            }

            override val encodedAs: EncodedAs
                get() = EncodedAs.BINARY

            // add cache for schema fingerprint
            private fun Schema.crc64avro(): ByteArray =
                ByteBuffer.allocate(8).putLong(SchemaNormalization.fingerprint64(SchemaNormalization.toParsingForm(this).encodeToByteArray())).array()

            override fun <T> encodeValue(avro: Avro2, writerSchema: Schema, serializer: SerializationStrategy<T>, value: T, outputStream: OutputStream) {
                outputStream.write(MAGIC_BYTE)
                outputStream.write(FORMAT_VERSION)
                outputStream.write(writerSchema.crc64avro())
                newEncoder(avro, writerSchema, serializer, outputStream)
                    .encodeSerializableValue(serializer, value)
            }

            override fun <T> decodeValue(avro: Avro2, readerSchema: Schema, deserializer: DeserializationStrategy<T>, inputStream: InputStream): T {
                check(inputStream.read() == MAGIC_BYTE) { "Not a valid single-object avro format, bad magic byte" }
                check(inputStream.read() == FORMAT_VERSION) { "Not a valid single-object avro format, bad version byte" }
                val writerSchema = findSchemaByFingerprint(ByteBuffer.wrap(ByteArray(8).apply { inputStream.read(this) }).order(ByteOrder.LITTLE_ENDIAN).getLong())

                checkNotNull(writerSchema) { "schema not found for the given object's schema fingerprint" }

                return newDecoder(avro, readerSchema, writerSchema, deserializer, inputStream)
                    .decodeSerializableValue(deserializer)
            }

            override fun <T> encodeSequence(avro: Avro2, writerSchema: Schema, serializer: SerializationStrategy<T>, sequence: Sequence<T>, outputStream: OutputStream) {
                throw UnsupportedOperationException("SingleObject avro format encoding does not support sequences")
            }

            override fun <T> decodeSequence(avro: Avro2, readerSchema: Schema, deserializer: DeserializationStrategy<T>, inputStream: InputStream): Sequence<T> {
                throw UnsupportedOperationException("SingleObject avro format encoding does not support sequences")
            }
        }
    }
}

/**
 * Extend this class when you need to customise the serialization and deserialization of avro, with a schema registry by example.
 */
@ExperimentalSerializationApi
abstract class CustomAvroSerializationDelegate : AvroSerializationDelegate()
