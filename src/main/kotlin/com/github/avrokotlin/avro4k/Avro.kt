package com.github.avrokotlin.avro4k

import com.github.avrokotlin.avro4k.decoder.AvroValueDecoder
import com.github.avrokotlin.avro4k.encoder.AvroValueEncoder
import com.github.avrokotlin.avro4k.internal.RecordResolver
import com.github.avrokotlin.avro4k.internal.UnionResolver
import com.github.avrokotlin.avro4k.schema.FieldNamingStrategy
import com.github.avrokotlin.avro4k.schema.ValueVisitor
import com.github.avrokotlin.avro4k.serializer.BigDecimalSerializer
import com.github.avrokotlin.avro4k.serializer.BigIntegerSerializer
import com.github.avrokotlin.avro4k.serializer.InstantSerializer
import com.github.avrokotlin.avro4k.serializer.LocalDateSerializer
import com.github.avrokotlin.avro4k.serializer.LocalDateTimeSerializer
import com.github.avrokotlin.avro4k.serializer.LocalTimeSerializer
import com.github.avrokotlin.avro4k.serializer.TimestampSerializer
import com.github.avrokotlin.avro4k.serializer.URLSerializer
import com.github.avrokotlin.avro4k.serializer.UUIDSerializer
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.modules.EmptySerializersModule
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.contextual
import kotlinx.serialization.modules.overwriteWith
import kotlinx.serialization.serializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericContainer
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
import org.apache.avro.reflect.ReflectDatumWriter
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.util.concurrent.ConcurrentHashMap

/**
 * The goal of this class is to serialize and deserialize in avro binary format, not in GenericRecords.
 */
sealed class Avro(
    val configuration: AvroConfiguration,
    val serializersModule: SerializersModule,
) {
    private val schemaCache: MutableMap<SerialDescriptor, Schema> = ConcurrentHashMap()
    internal val recordResolver = RecordResolver(this)
    internal val unionResolver = UnionResolver()

    companion object Default : Avro(
        AvroConfiguration(),
        SerializersModule {
            contextual(UUIDSerializer)
            contextual(URLSerializer)
            contextual(BigIntegerSerializer)
            contextual(BigDecimalSerializer)
            contextual(InstantSerializer)
            contextual(LocalDateSerializer)
            contextual(LocalTimeSerializer)
            contextual(LocalDateTimeSerializer)
            contextual(TimestampSerializer)
        }
    )

    fun schema(descriptor: SerialDescriptor): Schema {
        return schemaCache.getOrPut(descriptor) {
            lateinit var output: Schema
            ValueVisitor(this) { output = it }.visitValue(descriptor)
            return output
        }
    }

    fun <T> encodeToStream(
        writerSchema: Schema,
        serializer: SerializationStrategy<T>,
        value: T,
        outputStream: OutputStream,
    ) {
        val avroEncoder =
            when (configuration.encodedAs) {
                EncodedAs.BINARY -> EncoderFactory.get().binaryEncoder(outputStream, null)
                EncodedAs.JSON_COMPACT -> EncoderFactory.get().jsonEncoder(writerSchema, outputStream, false)
                EncodedAs.JSON_PRETTY -> EncoderFactory.get().jsonEncoder(writerSchema, outputStream, true)
            }
        val genericData = encodeToGenericData(writerSchema, serializer, value)
        ReflectDatumWriter<Any?>(writerSchema).write(genericData, avroEncoder)
        avroEncoder.flush()
    }

    fun <T> encodeToByteArray(
        writerSchema: Schema,
        serializer: SerializationStrategy<T>,
        value: T,
    ): ByteArray {
        val outputStream = ByteArrayOutputStream()
        encodeToStream(writerSchema, serializer, value, outputStream)
        return outputStream.toByteArray()
    }

    fun <T> encodeToGenericData(
        writerSchema: Schema,
        serializer: SerializationStrategy<T>,
        value: T,
    ): Any? {
        var result: Any? = null
        AvroValueEncoder(this, writerSchema) {
            result = it
        }.encodeSerializableValue(serializer, value)
        return result
    }

    fun <T> decodeFromStream(
        writerSchema: Schema,
        deserializer: DeserializationStrategy<T>,
        inputStream: InputStream,
    ): T {
        val avroDecoder =
            when (configuration.encodedAs) {
                EncodedAs.BINARY -> DecoderFactory.get().binaryDecoder(inputStream, null)
                EncodedAs.JSON_COMPACT, EncodedAs.JSON_PRETTY -> DecoderFactory.get().jsonDecoder(writerSchema, inputStream)
            }
        val genericData = GenericDatumReader<Any?>(writerSchema).read(null, avroDecoder)
        return decodeFromGenericData(writerSchema, deserializer, genericData)
    }

    fun <T> decodeFromByteArray(
        writerSchema: Schema,
        deserializer: DeserializationStrategy<T>,
        bytes: ByteArray,
    ): T {
        return decodeFromStream(writerSchema, deserializer, ByteArrayInputStream(bytes))
    }

    fun <T> decodeFromGenericData(
        writerSchema: Schema,
        deserializer: DeserializationStrategy<T>,
        value: Any?,
    ): T {
        return AvroValueDecoder(this, value, writerSchema)
            .decodeSerializableValue(deserializer)
    }
}

fun Avro(
    from: Avro = Avro,
    builderAction: AvroBuilder.() -> Unit,
): Avro {
    val builder = AvroBuilder(from)
    builder.builderAction()
    return AvroImpl(builder.build(), from.serializersModule.overwriteWith(builder.serializersModule))
}

class AvroBuilder internal constructor(avro: Avro) {
    var fieldNamingStrategy: FieldNamingStrategy = avro.configuration.fieldNamingStrategy
    var implicitNulls: Boolean = avro.configuration.implicitNulls
    var encodedAs: EncodedAs = avro.configuration.encodedAs
    var serializersModule: SerializersModule = EmptySerializersModule()

    fun build() =
        AvroConfiguration(
            fieldNamingStrategy = fieldNamingStrategy,
            implicitNulls = implicitNulls,
            encodedAs = encodedAs
        )
}

private class AvroImpl(configuration: AvroConfiguration, serializersModule: SerializersModule) :
    Avro(configuration, serializersModule)

// schema gen extensions

inline fun <reified T> Avro.schema(): Schema {
    val serializer = serializersModule.serializer<T>()
    return schema(serializer.descriptor)
}

fun <T> Avro.schema(serializer: KSerializer<T>): Schema {
    return schema(serializer.descriptor)
}

// encoding extensions

inline fun <reified T> Avro.encodeToByteArray(value: T): ByteArray {
    val serializer = serializersModule.serializer<T>()
    return encodeToByteArray(schema(serializer), serializer, value)
}

inline fun <reified T> Avro.encodeToByteArray(
    writerSchema: Schema,
    value: T,
): ByteArray {
    val serializer = serializersModule.serializer<T>()
    return encodeToByteArray(writerSchema, serializer, value)
}

inline fun <reified T> Avro.encodeToGenericData(value: T): Any? {
    val serializer = serializersModule.serializer<T>()
    return encodeToGenericData(schema(serializer), serializer, value)
}

inline fun <reified T> Avro.encodeToGenericData(
    writerSchema: Schema,
    value: T,
): Any? {
    val serializer = serializersModule.serializer<T>()
    return encodeToGenericData(writerSchema, serializer, value)
}

inline fun <reified T> Avro.encodeToStream(
    value: T,
    outputStream: OutputStream,
) {
    val serializer = serializersModule.serializer<T>()
    encodeToStream(schema(serializer), serializer, value, outputStream)
}

inline fun <reified T> Avro.encodeToStream(
    writerSchema: Schema,
    value: T,
    outputStream: OutputStream,
) {
    val serializer = serializersModule.serializer<T>()
    encodeToStream(writerSchema, serializer, value, outputStream)
}

// decoding extensions

inline fun <reified T> Avro.decodeFromStream(inputStream: InputStream): T {
    val serializer = serializersModule.serializer<T>()
    return decodeFromStream(schema(serializer.descriptor), serializer, inputStream)
}

inline fun <reified T> Avro.decodeFromStream(
    writerSchema: Schema,
    inputStream: InputStream,
): T {
    val serializer = serializersModule.serializer<T>()
    return decodeFromStream(writerSchema, serializer, inputStream)
}

inline fun <reified T> Avro.decodeFromByteArray(bytes: ByteArray): T {
    val serializer = serializersModule.serializer<T>()
    return decodeFromByteArray(schema(serializer.descriptor), serializer, bytes)
}

inline fun <reified T> Avro.decodeFromByteArray(
    writerSchema: Schema,
    bytes: ByteArray,
): T {
    val serializer = serializersModule.serializer<T>()
    return decodeFromByteArray(writerSchema, serializer, bytes)
}

inline fun <reified T> Avro.decodeFromGenericData(
    writerSchema: Schema,
    value: Any?,
): T {
    val deserializer = serializersModule.serializer<T>()
    return decodeFromGenericData(writerSchema, deserializer, value)
}

inline fun <reified T> Avro.decodeFromGenericData(value: GenericContainer?): T? {
    if (value == null) return null
    val deserializer = serializersModule.serializer<T>()
    return decodeFromGenericData(value.schema, deserializer, value)
}