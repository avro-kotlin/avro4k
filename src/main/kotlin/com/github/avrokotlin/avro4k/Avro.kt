@file:Suppress("DEPRECATION")

package com.github.avrokotlin.avro4k

import com.github.avrokotlin.avro4k.decoder.GenericAvroDecoder
import com.github.avrokotlin.avro4k.encoder.GenericAvroEncoder
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
import kotlinx.serialization.SerializationException
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.contextual
import kotlinx.serialization.serializer
import org.apache.avro.AvroRuntimeException
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
    private val converter: (Any) -> T
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
        val decodeFormatToUse = if (defaultDecodeFormat != currentDerivedFormat) {
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
    converter: (Any) -> T
) : AvroInputStreamBuilder<T>(converter) {
    val defaultReadSchema: Schema by lazy { avro.schema(deserializer.descriptor) }
}

class AvroOutputStreamBuilder<T>(
    private val serializer: SerializationStrategy<T>,
    private val avro: Avro,
    private val converterFn: (Schema) -> (T) -> Any?
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
        get() = when (format) {
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

    private fun createOutputStream(output: OutputStream, schema: Schema, converter: (T) -> Any?): AvroOutputStream<T> {
        val currentDirectEncodeFormat = derivedEncodeFormat
        val encodeFormatToUse = if (currentDirectEncodeFormat != defaultEncodeFormat) {
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
    internal val nameResolver = AvroNameResolver(this)

   constructor(configuration: AvroConfiguration) : this(defaultModule, configuration)

    companion object {
        val defaultModule = SerializersModule {
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
    override fun <T> decodeFromByteArray(deserializer: DeserializationStrategy<T>, bytes: ByteArray): T =
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
        f: AvroDeserializerInputStreamBuilder<T>.() -> Unit = {}
    ): AvroDeserializerInputStreamBuilder<T> {
        val defaultSchema by lazy { schema(deserializer.descriptor) }
        val builder = AvroDeserializerInputStreamBuilder(deserializer, this) {
            decodeFromGenericData(it, deserializer, (it as? GenericRecord)?.schema ?: defaultSchema)
                ?: throw SerializationException("Decoded record is null")
        }
        builder.f()
        return builder
    }


    /**
     * Writes an instance of <T> using a [Schema] derived from the type.
     * This method will use the [AvroEncodeFormat.Data] format without a codec.
     * The written object will be returned as a [ByteArray].
     */
    override fun <T> encodeToByteArray(serializer: SerializationStrategy<T>, value: T): ByteArray {
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
        f: AvroOutputStreamBuilder<T>.() -> Unit = {}
    ): AvroOutputStreamBuilder<T> {
        val builder = AvroOutputStreamBuilder(serializer, this) { schema -> { encodeToGenericData(
            it,
            serializer,
            schema
        ) } }
        builder.f()
        return builder
    }

    /**
     * Converts an instance of <T> to an Avro [Record] using a [Schema] derived from the type.
     */
    @Deprecated("Has been replaced by #encode that is able to not-only encode records", replaceWith = ReplaceWith("this.encode(serializer, obj)"))
    fun <T> toRecord(
        serializer: SerializationStrategy<T>,
        obj: T,
    ): GenericRecord {
        return toRecord(serializer, schema(serializer.descriptor), obj)
    }

    /**
     * Converts an instance of <T> to an Avro [Record] using the given [Schema].
     */
    @Deprecated("Has been replaced by #encode that is able to encode everything", replaceWith = ReplaceWith("this.encode(serializer, schema, obj)"))
    fun <T> toRecord(serializer: SerializationStrategy<T>,
                     schema: Schema,
                     obj: T): GenericRecord {
        val encodedValue = encodeToGenericData(obj, serializer, schema)
        if (encodedValue !is GenericRecord)
            throw AvroRuntimeException("Expected a GenericRecord, found $encodedValue")
        return encodedValue
    }

    fun <T> encodeToGenericData(value: T, serializer: SerializationStrategy<T>, schema: Schema): Any? =
        GenericAvroEncoder(schema, this).apply {
            resolveSchema(serializer.descriptor, value == null)
            encodeSerializableValue(serializer, value)
        }.encodedValue

    /**
     * Converts an Avro [GenericRecord] to an instance of <T> using the schema
     * present in the record.
     */
    @Deprecated("Has been replaced by #decodeFromGenericData that is able to decode everything", replaceWith = ReplaceWith("this.decodeFromGenericData(deserializer, record)"))
    fun <T> fromRecord(
        deserializer: DeserializationStrategy<T>,
        record: GenericRecord,
    ): T {
        return GenericAvroDecoder(record, record.schema, this)
            .decodeSerializableValue(deserializer)
    }

    fun <T> decodeFromGenericData(
        value: Any?,
        deserializer: DeserializationStrategy<T>,
        schema: Schema,
    ): T? {
        return GenericAvroDecoder(value, schema, this)
            .decodeNullableSerializableValue(deserializer)
    }

    fun schema(descriptor: SerialDescriptor): Schema = schemaFor(
        this,
        descriptor,
        descriptor.annotations,
        mutableMapOf()
    ).schema()

    fun <T> schema(serializer: SerializationStrategy<T>): Schema =
        schema(serializer.descriptor)
}

inline fun <reified T> Avro.encodeToGenericData(value: T): Any? {
    val serializer = serializersModule.serializer<T>()
    return encodeToGenericData(value, serializer, schema(serializer.descriptor))
}

fun <T> Avro.encodeToGenericData(value: T, serializer: SerializationStrategy<T>): Any? =
    encodeToGenericData(value, serializer, schema(serializer.descriptor))

inline fun <reified T> Avro.encodeToGenericData(value: T, schema: Schema) =
    encodeToGenericData(value, serializersModule.serializer(), schema)

inline fun <reified T> Avro.decodeFromGenericData(value: Any?): T? {
    val deserializer = serializersModule.serializer<T>()
    return decodeFromGenericData(value, deserializer, schema(deserializer.descriptor))
}

inline fun <reified T> Avro.decodeFromGenericData(value: Any?, schema: Schema): T? =
    decodeFromGenericData(value, serializersModule.serializer<T>(), schema)

inline fun <reified T> Avro.schema() =
    schema(serializersModule.serializer<T>().descriptor)
