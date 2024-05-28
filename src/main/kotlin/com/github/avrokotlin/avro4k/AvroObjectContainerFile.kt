package com.github.avrokotlin.avro4k

import com.github.avrokotlin.avro4k.internal.decodeWithBinaryDecoder
import com.github.avrokotlin.avro4k.internal.encodeWithBinaryEncoder
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.serializer
import org.apache.avro.Schema
import org.apache.avro.file.CodecFactory
import org.apache.avro.file.DataFileStream
import org.apache.avro.file.DataFileWriter
import org.apache.avro.io.DatumReader
import org.apache.avro.io.DatumWriter
import java.io.InputStream
import java.io.OutputStream

/**
 * Encode and decode values in object container files, also known as avro data file format.
 *
 * [spec](https://avro.apache.org/docs/1.11.1/specification/#object-container-files)
 */
@ExperimentalSerializationApi
public class AvroObjectContainerFile(
    @PublishedApi
    internal val avro: Avro = Avro,
) {
    public fun <T> encodeToStream(
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
    }

    public fun <T> decodeFromStream(
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

@ExperimentalSerializationApi
public inline fun <reified T> AvroObjectContainerFile.encodeToStream(
    values: Sequence<T>,
    outputStream: OutputStream,
    noinline builder: AvroObjectContainerFileBuilder.() -> Unit = {},
) {
    val serializer = avro.serializersModule.serializer<T>()
    encodeToStream(avro.schema(serializer), serializer, values, outputStream, builder)
}

@ExperimentalSerializationApi
public inline fun <reified T> AvroObjectContainerFile.decodeFromStream(
    inputStream: InputStream,
    noinline metadataDumper: AvroObjectContainerFileMetadataDumper.() -> Unit = {},
): Sequence<T> {
    val serializer = avro.serializersModule.serializer<T>()
    return decodeFromStream(serializer, inputStream, metadataDumper)
}

public class AvroObjectContainerFileBuilder(private val fileWriter: DataFileWriter<*>) {
    public fun metadata(
        key: String,
        value: ByteArray,
    ) {
        fileWriter.setMeta(key, value)
    }

    public fun metadata(
        key: String,
        value: String,
    ) {
        fileWriter.setMeta(key, value)
    }

    public fun metadata(
        key: String,
        value: Long,
    ) {
        fileWriter.setMeta(key, value)
    }

    public fun codec(codec: CodecFactory) {
        fileWriter.setCodec(codec)
    }
}

public class AvroObjectContainerFileMetadataDumper(private val fileStream: DataFileStream<*>) {
    public fun metadata(key: String): MetadataAccessor? {
        return fileStream.getMeta(key)?.let { MetadataAccessor(it) }
    }

    public inner class MetadataAccessor(private val value: ByteArray) {
        public fun asBytes(): ByteArray = value

        public fun asString(): String = value.decodeToString()

        public fun asLong(): Long = asString().toLong()
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
        avro.encodeWithBinaryEncoder(writerSchema, serializer, datum, encoder)
    }
}

private class KotlinxSerializationDatumReader<T>(
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
        return avro.decodeWithBinaryDecoder(writerSchema, deserializer, decoder)
    }
}