@file:Suppress("DEPRECATION")

package com.github.avrokotlin.avro4k

import com.github.avrokotlin.avro4k.decoder.RootRecordDecoder
import com.github.avrokotlin.avro4k.encoder.RootRecordEncoder
import com.github.avrokotlin.avro4k.io.AvroDecodeFormat
import com.github.avrokotlin.avro4k.io.AvroEncodeFormat
import com.github.avrokotlin.avro4k.io.AvroFormat
import com.github.avrokotlin.avro4k.io.AvroInputStream
import com.github.avrokotlin.avro4k.io.AvroOutputStream
import com.github.avrokotlin.avro4k.schema.schemaFor
import com.github.avrokotlin.avro4k.serializer.UUIDSerializer
import kotlinx.serialization.BinaryFormat
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerialFormat
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.contextual
import org.apache.avro.Schema
import org.apache.avro.file.CodecFactory
import org.apache.avro.generic.GenericRecord
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.InputStream
import java.io.OutputStream
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

open class AvroInputStreamBuilder<T>(
    private val converter: (Any) -> T,
) {
    /**
     * The format that should be used to decode the bytes from the input stream.
     */
    var decodeFormat: AvroDecodeFormat = defaultDecodeFormat

    @Deprecated("please use decodeFormat to specify the format")
    var format: AvroFormat = AvroFormat.DataFormat

    @Deprecated("please use decodeFormat to specify the format")
    var writerSchema: Schema? = null

    @Deprecated("please use decodeFormat to specify the format")
    var readerSchema: Schema? = null

    companion object {
        val defaultDecodeFormat = AvroDecodeFormat.Data(null, null)
    }

    private val derivedDecodeFormat: AvroDecodeFormat
        get() {
            return when (format) {
                is AvroFormat.JsonFormat -> {
                    val wschema = writerSchema ?: error("Writer schema needs to be supplied for Json format")
                    AvroDecodeFormat.Json(wschema, readerSchema ?: wschema)
                }
                is AvroFormat.BinaryFormat -> {
                    val wschema = writerSchema ?: error("Writer schema needs to be supplied for Binary format")
                    AvroDecodeFormat.Binary(wschema, readerSchema ?: wschema)
                }
                is AvroFormat.DataFormat -> AvroDecodeFormat.Data(writerSchema, readerSchema)
            }
        }

    fun from(path: Path): AvroInputStream<T> = from(Files.newInputStream(path))

    fun from(path: String): AvroInputStream<T> = from(Paths.get(path))

    fun from(file: File): AvroInputStream<T> = from(file.toPath())

    fun from(bytes: ByteArray): AvroInputStream<T> = from(ByteArrayInputStream(bytes))

    fun from(buffer: ByteBuffer): AvroInputStream<T> = from(ByteArrayInputStream(buffer.array()))

    fun from(source: InputStream): AvroInputStream<T> {
        val currentDerivedFormat = derivedDecodeFormat
        val decodeFormatToUse =
            if (defaultDecodeFormat != currentDerivedFormat) {
                currentDerivedFormat
            } else {
                decodeFormat
            }
        return decodeFormatToUse.createInputStream(source, converter)
    }
}

class AvroDeserializerInputStreamBuilder<T>(
    private val deserializer: DeserializationStrategy<T>,
    private val avro: Avro,
    converter: (Any) -> T,
) : AvroInputStreamBuilder<T>(converter) {
    val defaultReadSchema: Schema by lazy { avro.schema(deserializer.descriptor) }
}

class AvroOutputStreamBuilder<T>(
    private val serializer: SerializationStrategy<T>,
    private val avro: Avro,
    private val converterFn: (Schema) -> (T) -> GenericRecord,
) {
    var encodeFormat: AvroEncodeFormat = defaultEncodeFormat

    @Deprecated("please use AvroEncodeFormat to specify the format")
    var format: AvroFormat = AvroFormat.DataFormat
    var schema: Schema? = null

    @Deprecated("please use AvroEncodeFormat to specify the format")
    var codec: CodecFactory = CodecFactory.nullCodec()

    companion object {
        val defaultEncodeFormat = AvroEncodeFormat.Data()
    }

    private val derivedEncodeFormat: AvroEncodeFormat
        get() =
            when (format) {
                AvroFormat.JsonFormat -> AvroEncodeFormat.Json
                AvroFormat.BinaryFormat -> AvroEncodeFormat.Binary
                AvroFormat.DataFormat -> AvroEncodeFormat.Data(codec)
            }

    fun to(path: Path): AvroOutputStream<T> = to(Files.newOutputStream(path))

    fun to(path: String): AvroOutputStream<T> = to(Paths.get(path))

    fun to(file: File): AvroOutputStream<T> = to(file.toPath())

    fun to(output: OutputStream): AvroOutputStream<T> {
        val schema = schema ?: avro.schema(serializer)
        val converter = converterFn(schema)
        return createOutputStream(output, schema, converter)
    }

    private fun createOutputStream(
        output: OutputStream,
        schema: Schema,
        converter: (T) -> GenericRecord,
    ): AvroOutputStream<T> {
        val currentDirectEncodeFormat = derivedEncodeFormat
        val encodeFormatToUse =
            if (currentDirectEncodeFormat != defaultEncodeFormat) {
                currentDirectEncodeFormat
            } else {
                encodeFormat
            }
        return encodeFormatToUse.createOutputStream(output, schema, converter)
    }
}

@OptIn(ExperimentalSerializationApi::class)
class Avro(
    override val serializersModule: SerializersModule = defaultModule,
    internal val configuration: AvroConfiguration = AvroConfiguration(),
) : SerialFormat, BinaryFormat {
    constructor(configuration: AvroConfiguration) : this(defaultModule, configuration)

    companion object {
        val defaultModule =
            SerializersModule {
                contextual(UUIDSerializer())
            }
        val default = Avro(defaultModule)

        /**
         * Use this constant if you want to explicitly set a default value of a field to avro null
         */
        const val NULL = "com.github.avrokotlin.avro4k.Avro.AVRO_NULL_DEFAULT"
    }

    /**
     * Loads an instance of <T> from the given ByteArray, with the assumption that the record was stored
     * using [AvroEncodeFormat.Data]. The schema used will be the embedded schema.
     */
    override fun <T> decodeFromByteArray(
        deserializer: DeserializationStrategy<T>,
        bytes: ByteArray,
    ): T =
        openInputStream(deserializer) {
            decodeFormat = AvroDecodeFormat.Data(null, null)
        }.from(bytes).nextOrThrow()

    /**
     * Creates an [AvroInputStreamBuilder] that will read avro values such as GenericRecord.
     * Supply a function to this method to configure the builder, eg
     *
     * <pre>
     * val input = openInputStream<T>(serializer) {
     *    decodeFormat = AvroDecodeFormat.Data(writerSchema = null, readerSchema = mySchema)
     * }
     * </pre>
     */
    fun openInputStream(f: AvroInputStreamBuilder<Any>.() -> Unit = {}): AvroInputStreamBuilder<Any> {
        val builder = AvroInputStreamBuilder { it }
        builder.f()
        return builder
    }

    /**
     * Creates an [AvroInputStreamBuilder] that will read instances of <T>.
     * Supply a function to this method to configure the builder, eg
     *
     * <pre>
     * val input = openInputStream<T>(serializer) {
     *    decodeFormat = AvroDecodeFormat.Data(writerSchema = null, readerSchema = mySchema)
     * }
     * </pre>
     */
    fun <T> openInputStream(
        deserializer: DeserializationStrategy<T>,
        f: AvroDeserializerInputStreamBuilder<T>.() -> Unit = {},
    ): AvroDeserializerInputStreamBuilder<T> {
        val builder =
            AvroDeserializerInputStreamBuilder(deserializer, this) {
                fromRecord(deserializer, it as GenericRecord)
            }
        builder.f()
        return builder
    }

    /**
     * Writes an instance of <T> using a [Schema] derived from the type.
     * This method will use the [AvroEncodeFormat.Data] format without a codec.
     * The written object will be returned as a [ByteArray].
     */
    override fun <T> encodeToByteArray(
        serializer: SerializationStrategy<T>,
        value: T,
    ): ByteArray {
        val baos = ByteArrayOutputStream()
        openOutputStream(serializer) {
            encodeFormat = AvroEncodeFormat.Data()
        }.to(baos).write(value).close()
        return baos.toByteArray()
    }

    /**
     * Creates an [AvroOutputStreamBuilder] that will write instances of <T>.
     * Supply a function to this method to configure the builder, eg
     *
     * <pre>
     * val output = openOutputStream<T>(serializer) {
     *    encodeFormat = AvroEncodeFormat.Data()
     *    schema = mySchema
     * }
     * </pre>
     *
     * If the schema is not supplied in the configuration function then it will
     * be derived from the type using [Avro.schema].
     */
    fun <T> openOutputStream(
        serializer: SerializationStrategy<T>,
        f: AvroOutputStreamBuilder<T>.() -> Unit = {},
    ): AvroOutputStreamBuilder<T> {
        val builder = AvroOutputStreamBuilder(serializer, this) { schema -> { toRecord(serializer, schema, it) } }
        builder.f()
        return builder
    }

    /**
     * Converts an instance of <T> to an Avro [Record] using a [Schema] derived from the type.
     */
    fun <T> toRecord(
        serializer: SerializationStrategy<T>,
        obj: T,
    ): GenericRecord {
        return toRecord(serializer, schema(serializer), obj)
    }

    /**
     * Converts an instance of <T> to an Avro [Record] using the given [Schema].
     */
    fun <T> toRecord(
        serializer: SerializationStrategy<T>,
        schema: Schema,
        obj: T,
    ): GenericRecord {
        var record: Record? = null
        val encoder = RootRecordEncoder(schema, serializersModule, configuration) { record = it }
        encoder.encodeSerializableValue(serializer, obj)
        return record!!
    }

    /**
     * Converts an Avro [GenericRecord] to an instance of <T> using the schema
     * present in the record.
     */
    fun <T> fromRecord(
        deserializer: DeserializationStrategy<T>,
        record: GenericRecord,
    ): T {
        return RootRecordDecoder(record, serializersModule, configuration).decodeSerializableValue(
            deserializer
        )
    }

    fun schema(descriptor: SerialDescriptor): Schema =
        schemaFor(
            serializersModule,
            descriptor,
            descriptor.annotations,
            configuration,
            mutableMapOf()
        ).schema()

    fun <T> schema(serializer: SerializationStrategy<T>): Schema {
        return schema(serializer.descriptor)
    }
}