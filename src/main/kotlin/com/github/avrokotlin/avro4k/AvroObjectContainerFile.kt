package com.github.avrokotlin.avro4k

import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.serializer
import org.apache.avro.Schema
import org.apache.avro.file.CodecFactory
import org.apache.avro.file.DataFileStream
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.io.DatumReader
import org.apache.avro.io.DatumWriter
import org.apache.avro.reflect.ReflectDatumWriter
import java.io.InputStream
import java.io.OutputStream

/**
 * Encode and decode values in object container files, also known as avro data file format.
 *
 * [spec](https://avro.apache.org/docs/1.11.1/specification/#object-container-files)
 */
@ExperimentalSerializationApi
class AvroObjectContainerFile(
    val avro: Avro = Avro,
) {
    fun <T> encodeToStream(
        schema: Schema,
        serializer: SerializationStrategy<T>,
        values: Sequence<T>,
        outputStream: OutputStream,
        builder: AvroObjectContainerFileBuilder.() -> Unit = {},
    ) {
        val datumWriter: DatumWriter<T> = KotlinxSerializationDatumWriter(serializer, avro)
        val dataFileWriter = DataFileWriter(datumWriter)

        builder(AvroObjectContainerFileBuilder(dataFileWriter))
        dataFileWriter.create(schema, outputStream)
        values.forEach {
            dataFileWriter.append(it)
        }
        dataFileWriter.flush()
        // don't close the stream, the caller should do it
    }

    fun <T> decodeFromStream(
        deserializer: DeserializationStrategy<T>,
        inputStream: InputStream,
        metadataDumper: AvroObjectContainerFileMetadataDumper.() -> Unit = {},
    ): Sequence<T> =
        sequence {
            val datumReader: DatumReader<T> = KotlinxSerializationDatumReader(deserializer, avro)
            DataFileStream(inputStream, datumReader).use { dataFileStream ->
                metadataDumper(AvroObjectContainerFileMetadataDumper(dataFileStream))
                yieldAll(dataFileStream.iterator())
            }
        }.constrainOnce()
}

inline fun <reified T> AvroObjectContainerFile.encodeToStream(
    values: Sequence<T>,
    outputStream: OutputStream,
    noinline builder: AvroObjectContainerFileBuilder.() -> Unit = {},
) {
    val serializer = avro.serializersModule.serializer<T>()
    encodeToStream(avro.schema(serializer), serializer, values, outputStream, builder)
}

inline fun <reified T> AvroObjectContainerFile.decodeFromStream(
    inputStream: InputStream,
    noinline metadataDumper: AvroObjectContainerFileMetadataDumper.() -> Unit = {},
): Sequence<T> {
    val serializer = avro.serializersModule.serializer<T>()
    return decodeFromStream(serializer, inputStream, metadataDumper)
}

class AvroObjectContainerFileBuilder(private val fileWriter: DataFileWriter<*>) {
    fun metadata(
        key: String,
        value: ByteArray,
    ) {
        fileWriter.setMeta(key, value)
    }

    fun metadata(
        key: String,
        value: String,
    ) {
        fileWriter.setMeta(key, value)
    }

    fun metadata(
        key: String,
        value: Long,
    ) {
        fileWriter.setMeta(key, value)
    }

    fun codec(codec: CodecFactory) {
        fileWriter.setCodec(codec)
    }
}

class AvroObjectContainerFileMetadataDumper(private val fileStream: DataFileStream<*>) {
    fun metadata(key: String): MetadataAccessor? {
        return fileStream.getMeta(key)?.let { MetadataAccessor(it) }
    }

    inner class MetadataAccessor(private val value: ByteArray) {
        fun asBytes(): ByteArray = value

        fun asString(): String = value.decodeToString()

        fun asLong(): Long = asString().toLong()
    }
}

private class KotlinxSerializationDatumWriter<T>(
    private val serializer: SerializationStrategy<T>,
    private val avro: Avro,
) : DatumWriter<T> {
    private lateinit var writerSchema: Schema

    override fun setSchema(schema: Schema) {
        writerSchema = schema
    }

    override fun write(
        datum: T,
        encoder: org.apache.avro.io.Encoder,
    ) {
        val genericData = avro.encodeToGenericData(writerSchema, serializer, datum)
        ReflectDatumWriter<Any?>(writerSchema).write(genericData, encoder)
    }
}

internal class KotlinxSerializationDatumReader<T>(
    private val deserializer: DeserializationStrategy<T>,
    private val avro: Avro,
) : DatumReader<T> {
    private lateinit var writerSchema: Schema

    override fun setSchema(schema: Schema) {
        writerSchema = schema
    }

    override fun read(
        reuse: T?,
        decoder: org.apache.avro.io.Decoder,
    ): T {
        val genericData = GenericDatumReader<Any?>(writerSchema).read(reuse, decoder)
        return avro.decodeFromGenericData(writerSchema, deserializer, genericData)
    }
}