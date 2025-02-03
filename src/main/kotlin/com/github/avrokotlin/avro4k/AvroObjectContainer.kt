package com.github.avrokotlin.avro4k

import com.github.avrokotlin.avro4k.internal.decodeWithApacheDecoder
import com.github.avrokotlin.avro4k.internal.encodeWithApacheEncoder
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
import java.io.Closeable
import java.io.InputStream
import java.io.OutputStream

/**
 * Encode and decode values in object container files, also known as avro data file format.
 *
 * [spec](https://avro.apache.org/docs/1.11.1/specification/#object-container-files)
 */
@ExperimentalSerializationApi
public sealed class AvroObjectContainer(
    @PublishedApi
    internal val avro: Avro,
) {
    public companion object Default : AvroObjectContainer(Avro)

    /**
     * Opens a writer to allow encoding values to avro and writing them to the output stream.
     *
     * Note that the output stream is not closed after the operation, which means you need to handle it to avoid resource leaks.
     */
    public fun <T> openWriter(
        schema: Schema,
        serializer: SerializationStrategy<T>,
        outputStream: OutputStream,
        builder: AvroObjectContainerBuilder.() -> Unit = {},
    ): AvroObjectContainerWriter<T> {
        val datumWriter: DatumWriter<T> = KotlinxSerializationDatumWriter(serializer, avro)
        val dataFileWriter = DataFileWriter(datumWriter)
        builder(AvroObjectContainerBuilder(dataFileWriter))
        dataFileWriter.create(schema, outputStream)
        return AvroObjectContainerWriter(dataFileWriter)
    }

    public fun <T> decodeFromStream(
        deserializer: DeserializationStrategy<T>,
        inputStream: InputStream,
        metadataDumper: AvroObjectContainerMetadataDumper.() -> Unit = {},
    ): Sequence<T> {
        return sequence {
            val datumReader: DatumReader<T> = KotlinxSerializationDatumReader(deserializer, avro)
            val dataFileStream = DataFileStream(inputStream, datumReader)
            metadataDumper(AvroObjectContainerMetadataDumper(dataFileStream))
            yieldAll(dataFileStream.iterator())
        }.constrainOnce()
    }
}

public class AvroObjectContainerWriter<T> internal constructor(
    private val writer: DataFileWriter<T>,
) : Closeable {
    public fun writeValue(value: T) {
        writer.append(value)
    }

    override fun close() {
        writer.flush()
    }
}

private class AvroObjectContainerImpl(avro: Avro) : AvroObjectContainer(avro)

public fun AvroObjectContainer(
    from: Avro = Avro,
    builderAction: AvroBuilder.() -> Unit,
): AvroObjectContainer {
    return AvroObjectContainerImpl(Avro(from, builderAction))
}

@ExperimentalSerializationApi
public inline fun <reified T> AvroObjectContainer.openWriter(
    outputStream: OutputStream,
    noinline builder: AvroObjectContainerBuilder.() -> Unit = {},
): AvroObjectContainerWriter<T> {
    val serializer = avro.serializersModule.serializer<T>()
    return openWriter(avro.schema(serializer), serializer, outputStream, builder)
}

@ExperimentalSerializationApi
public inline fun <reified T> AvroObjectContainer.decodeFromStream(
    inputStream: InputStream,
    noinline metadataDumper: AvroObjectContainerMetadataDumper.() -> Unit = {},
): Sequence<T> {
    val serializer = avro.serializersModule.serializer<T>()
    return decodeFromStream(serializer, inputStream, metadataDumper)
}

public class AvroObjectContainerBuilder internal constructor(private val fileWriter: DataFileWriter<*>) {
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

    public fun syncInterval(value: Int) {
        fileWriter.setSyncInterval(value)
    }

    public fun codec(codec: CodecFactory) {
        fileWriter.setCodec(codec)
    }
}

public class AvroObjectContainerMetadataDumper internal constructor(private val fileStream: DataFileStream<*>) {
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
        avro.encodeWithApacheEncoder(writerSchema, serializer, datum, encoder)
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
        return avro.decodeWithApacheDecoder(writerSchema, deserializer, decoder)
    }
}