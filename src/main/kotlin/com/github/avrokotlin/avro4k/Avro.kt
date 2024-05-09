package com.github.avrokotlin.avro4k

import com.github.avrokotlin.avro4k.decoder.AvroValueDecoder
import com.github.avrokotlin.avro4k.encoder.AvroValueEncoder
import com.github.avrokotlin.avro4k.internal.EnumResolver
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
import org.apache.avro.util.WeakIdentityHashMap
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.OutputStream

/**
 * The goal of this class is to serialize and deserialize in avro binary format, not in GenericRecords.
 */
public sealed class Avro(
    public val configuration: AvroConfiguration,
    public val serializersModule: SerializersModule,
) {
    // We use the identity hash map because we could have multiple descriptors with the same name, especially
    // when having 2 different version of the schema for the same name. kotlinx-serialization is instanciating the descriptors
    // only once, so we are safe in the main use cases. Combined with weak references to avoid memory leaks.
    private val schemaCache: MutableMap<SerialDescriptor, Schema> = WeakIdentityHashMap()

    internal val recordResolver = RecordResolver(this)
    internal val unionResolver = UnionResolver()
    internal val enumResolver = EnumResolver()

    public companion object Default : Avro(
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
        }
    )

    public fun schema(descriptor: SerialDescriptor): Schema {
        return schemaCache.getOrPut(descriptor) {
            lateinit var output: Schema
            ValueVisitor(this) { output = it }.visitValue(descriptor)
            output
        }
    }

    public fun <T> encodeToStream(
        writerSchema: Schema,
        serializer: SerializationStrategy<T>,
        value: T,
        outputStream: OutputStream,
    ) {
        val avroEncoder = EncoderFactory.get().directBinaryEncoder(outputStream, null)
        val genericData = encodeToGenericData(writerSchema, serializer, value)
        ReflectDatumWriter<Any?>(writerSchema).write(genericData, avroEncoder)
        avroEncoder.flush()
    }

    public fun <T> encodeToByteArray(
        writerSchema: Schema,
        serializer: SerializationStrategy<T>,
        value: T,
    ): ByteArray {
        val outputStream = ByteArrayOutputStream()
        encodeToStream(writerSchema, serializer, value, outputStream)
        return outputStream.toByteArray()
    }

    public fun <T> encodeToGenericData(
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

    public fun <T> decodeFromStream(
        writerSchema: Schema,
        deserializer: DeserializationStrategy<T>,
        inputStream: InputStream,
    ): T {
        val avroDecoder = DecoderFactory.get().directBinaryDecoder(inputStream, null)
        val genericData = GenericDatumReader<Any?>(writerSchema).read(null, avroDecoder)
        return decodeFromGenericData(writerSchema, deserializer, genericData)
    }

    public fun <T> decodeFromByteArray(
        writerSchema: Schema,
        deserializer: DeserializationStrategy<T>,
        bytes: ByteArray,
    ): T {
        return decodeFromStream(writerSchema, deserializer, ByteArrayInputStream(bytes))
    }

    public fun <T> decodeFromGenericData(
        writerSchema: Schema,
        deserializer: DeserializationStrategy<T>,
        value: Any?,
    ): T {
        return AvroValueDecoder(this, value, writerSchema)
            .decodeSerializableValue(deserializer)
    }
}

public fun Avro(
    from: Avro = Avro,
    builderAction: AvroBuilder.() -> Unit,
): Avro {
    val builder = AvroBuilder(from)
    builder.builderAction()
    return AvroImpl(builder.build(), from.serializersModule.overwriteWith(builder.serializersModule))
}

public class AvroBuilder internal constructor(avro: Avro) {
    public var fieldNamingStrategy: FieldNamingStrategy = avro.configuration.fieldNamingStrategy
    public var implicitNulls: Boolean = avro.configuration.implicitNulls
    public var serializersModule: SerializersModule = EmptySerializersModule()

    public fun build(): AvroConfiguration =
        AvroConfiguration(
            fieldNamingStrategy = fieldNamingStrategy,
            implicitNulls = implicitNulls
        )
}

private class AvroImpl(configuration: AvroConfiguration, serializersModule: SerializersModule) :
    Avro(configuration, serializersModule)

// schema gen extensions

public inline fun <reified T> Avro.schema(): Schema {
    val serializer = serializersModule.serializer<T>()
    return schema(serializer.descriptor)
}

public fun <T> Avro.schema(serializer: KSerializer<T>): Schema {
    return schema(serializer.descriptor)
}

// encoding extensions

public inline fun <reified T> Avro.encodeToByteArray(value: T): ByteArray {
    val serializer = serializersModule.serializer<T>()
    return encodeToByteArray(schema(serializer), serializer, value)
}

public inline fun <reified T> Avro.encodeToByteArray(
    writerSchema: Schema,
    value: T,
): ByteArray {
    val serializer = serializersModule.serializer<T>()
    return encodeToByteArray(writerSchema, serializer, value)
}

public inline fun <reified T> Avro.encodeToGenericData(value: T): Any? {
    val serializer = serializersModule.serializer<T>()
    return encodeToGenericData(schema(serializer), serializer, value)
}

public inline fun <reified T> Avro.encodeToGenericData(
    writerSchema: Schema,
    value: T,
): Any? {
    val serializer = serializersModule.serializer<T>()
    return encodeToGenericData(writerSchema, serializer, value)
}

public inline fun <reified T> Avro.encodeToStream(
    value: T,
    outputStream: OutputStream,
) {
    val serializer = serializersModule.serializer<T>()
    encodeToStream(schema(serializer), serializer, value, outputStream)
}

public inline fun <reified T> Avro.encodeToStream(
    writerSchema: Schema,
    value: T,
    outputStream: OutputStream,
) {
    val serializer = serializersModule.serializer<T>()
    encodeToStream(writerSchema, serializer, value, outputStream)
}

// decoding extensions

public inline fun <reified T> Avro.decodeFromStream(inputStream: InputStream): T {
    val serializer = serializersModule.serializer<T>()
    return decodeFromStream(schema(serializer.descriptor), serializer, inputStream)
}

public inline fun <reified T> Avro.decodeFromStream(
    writerSchema: Schema,
    inputStream: InputStream,
): T {
    val serializer = serializersModule.serializer<T>()
    return decodeFromStream(writerSchema, serializer, inputStream)
}

public inline fun <reified T> Avro.decodeFromByteArray(bytes: ByteArray): T {
    val serializer = serializersModule.serializer<T>()
    return decodeFromByteArray(schema(serializer.descriptor), serializer, bytes)
}

public inline fun <reified T> Avro.decodeFromByteArray(
    writerSchema: Schema,
    bytes: ByteArray,
): T {
    val serializer = serializersModule.serializer<T>()
    return decodeFromByteArray(writerSchema, serializer, bytes)
}

public inline fun <reified T> Avro.decodeFromGenericData(
    writerSchema: Schema,
    value: Any?,
): T {
    val deserializer = serializersModule.serializer<T>()
    return decodeFromGenericData(writerSchema, deserializer, value)
}

public inline fun <reified T> Avro.decodeFromGenericData(value: GenericContainer?): T? {
    if (value == null) return null
    val deserializer = serializersModule.serializer<T>()
    return decodeFromGenericData(value.schema, deserializer, value)
}