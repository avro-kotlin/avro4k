package com.github.avrokotlin.avro4k

import com.github.avrokotlin.avro4k.internal.EnumResolver
import com.github.avrokotlin.avro4k.internal.PolymorphicResolver
import com.github.avrokotlin.avro4k.internal.RecordResolver
import com.github.avrokotlin.avro4k.internal.decodeWithApacheDecoder
import com.github.avrokotlin.avro4k.internal.schema.ValueVisitor
import com.github.avrokotlin.avro4k.serializer.JavaStdLibSerializersModule
import com.github.avrokotlin.avro4k.serializer.JavaTimeSerializersModule
import kotlinx.io.Buffer
import kotlinx.io.UnsafeIoApi
import kotlinx.io.readByteArray
import kotlinx.io.unsafe.UnsafeBufferOperations
import kotlinx.serialization.BinaryFormat
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationException
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.modules.EmptySerializersModule
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.overwriteWith
import kotlinx.serialization.modules.plus
import kotlinx.serialization.serializer
import org.apache.avro.Schema
import org.apache.avro.io.DecoderFactory
import org.apache.avro.util.WeakIdentityHashMap

/**
 * The goal of this class is to serialize and deserialize in avro binary format, not in GenericRecords.
 */
public sealed class Avro(
    public val configuration: AvroConfiguration,
    public final override val serializersModule: SerializersModule,
) : BinaryFormat {
    // We use the identity hash map because we could have multiple descriptors with the same name, especially
    // when having 2 different version of the schema for the same name. kotlinx-serialization is instantiating the descriptors
    // only once, so we are safe in the main use cases. Combined with weak references to avoid memory leaks.
    private val schemaCache: MutableMap<SerialDescriptor, Schema> = WeakIdentityHashMap()

    internal val recordResolver = RecordResolver(this)
    internal val polymorphicResolver = PolymorphicResolver(serializersModule)
    internal val enumResolver = EnumResolver()

    public companion object Default : Avro(
        AvroConfiguration(),
        JavaStdLibSerializersModule +
                JavaTimeSerializersModule
    )

    public fun schema(descriptor: SerialDescriptor): Schema {
        return schemaCache.getOrPut(descriptor) {
            lateinit var output: Schema
            ValueVisitor(this) { output = it }.visitValue(descriptor)
            output
        }
    }

    public fun <T> encodeToByteArray(
        writerSchema: Schema,
        serializer: SerializationStrategy<T>,
        value: T,
    ): ByteArray {
        val buffer = Buffer()
        encodeToSink(writerSchema, serializer, value, buffer)
        return buffer.readByteArray()
    }

    public fun <T> decodeFromByteArray(
        writerSchema: Schema,
        deserializer: DeserializationStrategy<T>,
        bytes: ByteArray,
    ): T {
        val source = Buffer(bytes)
        val result = decodeFromSource(writerSchema, deserializer, source)
        if (!source.exhausted()) {
            throw SerializationException("Not all bytes were consumed during deserialization")
        }
        return result
    }

    override fun <T> decodeFromByteArray(
        deserializer: DeserializationStrategy<T>,
        bytes: ByteArray,
    ): T {
        return decodeFromByteArray(schema(deserializer.descriptor), deserializer, bytes)
    }

    override fun <T> encodeToByteArray(
        serializer: SerializationStrategy<T>,
        value: T,
    ): ByteArray {
        return encodeToByteArray(schema(serializer.descriptor), serializer, value)
    }
}

@OptIn(UnsafeIoApi::class)
private fun Buffer(bytes: ByteArray): Buffer = Buffer().apply {
    UnsafeBufferOperations.moveToTail(this, bytes)
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
    @ExperimentalSerializationApi
    public var fieldNamingStrategy: FieldNamingStrategy = avro.configuration.fieldNamingStrategy

    @ExperimentalSerializationApi
    public var implicitNulls: Boolean = avro.configuration.implicitNulls

    @ExperimentalSerializationApi
    public var implicitEmptyCollections: Boolean = avro.configuration.implicitEmptyCollections

    @ExperimentalSerializationApi
    public var validateSerialization: Boolean = avro.configuration.validateSerialization
    public var serializersModule: SerializersModule = EmptySerializersModule()

    internal fun build(): AvroConfiguration =
        AvroConfiguration(
            fieldNamingStrategy = fieldNamingStrategy,
            implicitNulls = implicitNulls,
            implicitEmptyCollections = implicitEmptyCollections,
            validateSerialization = validateSerialization
        )
}

private class AvroImpl(configuration: AvroConfiguration, serializersModule: SerializersModule) :
    Avro(configuration, serializersModule)

public inline fun <reified T> Avro.schema(): Schema {
    val serializer = serializersModule.serializer<T>()
    return schema(serializer.descriptor)
}

public fun <T> Avro.schema(serializer: KSerializer<T>): Schema {
    return schema(serializer.descriptor)
}

public inline fun <reified T> Avro.encodeToByteArray(
    writerSchema: Schema,
    value: T,
): ByteArray {
    val serializer = serializersModule.serializer<T>()
    return encodeToByteArray(writerSchema, serializer, value)
}

public inline fun <reified T> Avro.decodeFromByteArray(
    writerSchema: Schema,
    bytes: ByteArray,
): T {
    val serializer = serializersModule.serializer<T>()
    return decodeFromByteArray(writerSchema, serializer, bytes)
}