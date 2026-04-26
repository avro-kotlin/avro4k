@file:OptIn(kotlin.uuid.ExperimentalUuidApi::class)

package com.github.avrokotlin.avro4k.internal

import com.github.avrokotlin.avro4k.AvroDecoder
import com.github.avrokotlin.avro4k.AvroEncoder
import com.github.avrokotlin.avro4k.internal.decoder.direct.AbstractAvroDirectDecoder
import com.github.avrokotlin.avro4k.serializer.AvroDuration
import com.github.avrokotlin.avro4k.serializer.AvroDurationSerializer
import com.github.avrokotlin.avro4k.serializer.AvroSerializer
import com.github.avrokotlin.avro4k.serializer.KotlinInstantSerializer
import com.github.avrokotlin.avro4k.serializer.KotlinUuidSerializer
import com.github.avrokotlin.avro4k.serializer.SchemaSupplierContext
import com.github.avrokotlin.avro4k.serializer.SerialDescriptorWithAvroSchemaDelegate
import com.github.avrokotlin.avro4k.serializer.createSchema
import com.github.avrokotlin.avro4k.serializer.fixed
import com.github.avrokotlin.avro4k.serializer.stringable
import kotlinx.datetime.Instant
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationException
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.builtins.ByteArraySerializer
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.internal.AbstractCollectionSerializer
import org.apache.avro.Schema
import kotlin.io.encoding.Base64
import kotlin.io.encoding.ExperimentalEncodingApi
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.milliseconds
import kotlin.uuid.Uuid

/**
 * This middleware is here to intercept some native types like kotlin Duration or ByteArray as we want to apply some
 * specific rules on them for generating custom schemas or having specific serialization strategies.
 */
@Suppress("UNCHECKED_CAST")
internal object SerializerLocatorMiddleware {
    fun <T> apply(serializer: SerializationStrategy<T>): SerializationStrategy<T> {
        return when {
            serializer === ByteArraySerializer() -> AvroByteArraySerializer
            serializer === Duration.serializer() -> KotlinDurationSerializer
            serializer === Uuid.serializer() -> KotlinUuidSerializer
            serializer === Instant.serializer() -> KotlinInstantSerializer
            else -> serializer
        } as SerializationStrategy<T>
    }

    @OptIn(InternalSerializationApi::class)
    fun <T> apply(deserializer: DeserializationStrategy<T>): DeserializationStrategy<T> {
        return when {
            deserializer === ByteArraySerializer() -> AvroByteArraySerializer
            deserializer === Duration.serializer() -> KotlinDurationSerializer
            deserializer === Uuid.serializer() -> KotlinUuidSerializer
            deserializer === Instant.serializer() -> KotlinInstantSerializer
            deserializer is AbstractCollectionSerializer<*, T, *> -> AvroCollectionSerializer(deserializer)
            else -> deserializer
        } as DeserializationStrategy<T>
    }

    fun apply(descriptor: SerialDescriptor): SerialDescriptor {
        return when {
            descriptor === ByteArraySerializer().descriptor -> AvroByteArraySerializer.descriptor
            descriptor === String.serializer().descriptor -> AvroStringSerialDescriptor
            descriptor === Duration.serializer().descriptor -> KotlinDurationSerializer.descriptor
            descriptor === Uuid.serializer().descriptor -> KotlinUuidSerializer.descriptor
            descriptor === Instant.serializer().descriptor -> KotlinInstantSerializer.descriptor
            else -> descriptor
        }
    }
}

private val AvroStringSerialDescriptor: SerialDescriptor =
    SerialDescriptorWithAvroSchemaDelegate(String.serializer().descriptor) { context ->
        context.inlinedElements.firstNotNullOfOrNull {
            it.stringable?.createSchema() ?: it.fixed?.createSchema(it)
        } ?: Schema.create(Schema.Type.STRING)
    }

private object KotlinDurationSerializer : AvroSerializer<Duration>(Duration::class.qualifiedName!!) {
    private const val MILLIS_PER_DAY = 1000 * 60 * 60 * 24

    override fun getSchema(context: SchemaSupplierContext): Schema {
        return context.inlinedElements.firstNotNullOfOrNull { it.stringable?.createSchema() }
            ?: AvroDurationSerializer.DURATION_SCHEMA
    }

    override fun serializeAvro(
        encoder: AvroEncoder,
        value: Duration,
    ) {
        AvroDurationSerializer.serializeAvro(encoder, value.toAvroDuration())
    }

    override fun deserializeAvro(decoder: AvroDecoder): Duration {
        return AvroDurationSerializer.deserializeAvro(decoder).toKotlinDuration()
    }

    override fun serializeGeneric(
        encoder: Encoder,
        value: Duration,
    ) {
        encoder.encodeString(value.toString())
    }

    override fun deserializeGeneric(decoder: Decoder): Duration {
        return Duration.parse(decoder.decodeString())
    }

    private fun AvroDuration.toKotlinDuration(): Duration {
        if (months == UInt.MAX_VALUE && days == UInt.MAX_VALUE && millis == UInt.MAX_VALUE) {
            return Duration.INFINITE
        }
        if (months != 0u) {
            throw SerializationException("java.time.Duration cannot contains months")
        }
        return days.toLong().days + millis.toLong().milliseconds
    }

    private fun Duration.toAvroDuration(): AvroDuration {
        if (isNegative()) {
            throw SerializationException("${Duration::class.qualifiedName} cannot be converted to ${AvroDuration::class.qualifiedName} as it cannot be negative")
        }
        if (isInfinite()) {
            return AvroDuration(
                months = UInt.MAX_VALUE,
                days = UInt.MAX_VALUE,
                millis = UInt.MAX_VALUE
            )
        }
        val millis = inWholeMilliseconds
        return AvroDuration(
            months = 0u,
            days = (millis / MILLIS_PER_DAY).toUInt(),
            millis = (millis % MILLIS_PER_DAY).toUInt()
        )
    }
}

private object AvroByteArraySerializer : AvroSerializer<ByteArray>(ByteArray::class.qualifiedName!!) {
    override fun getSchema(context: SchemaSupplierContext): Schema {
        return context.inlinedElements.firstNotNullOfOrNull {
            it.stringable?.createSchema() ?: it.fixed?.createSchema(it)
        } ?: Schema.create(Schema.Type.BYTES)
    }

    override fun serializeAvro(
        encoder: AvroEncoder,
        value: ByteArray,
    ) {
        // encoding related to the type (fixed or bytes) is handled in AvroEncoder
        encoder.encodeBytes(value)
    }

    override fun deserializeAvro(decoder: AvroDecoder): ByteArray {
        // decoding related to the type (fixed or bytes) is handled in AvroDecoder
        return decoder.decodeBytes()
    }

    @OptIn(ExperimentalEncodingApi::class)
    override fun serializeGeneric(
        encoder: Encoder,
        value: ByteArray,
    ) {
        encoder.encodeString(Base64.Mime.encode(value))
    }

    @OptIn(ExperimentalEncodingApi::class)
    override fun deserializeGeneric(decoder: Decoder): ByteArray {
        return Base64.Mime.decode(decoder.decodeString())
    }
}

@OptIn(InternalSerializationApi::class)
internal class AvroCollectionSerializer<T>(private val original: AbstractCollectionSerializer<*, T, *>) : KSerializer<T> {
    override val descriptor: SerialDescriptor
        get() = original.descriptor

    override fun deserialize(decoder: Decoder): T {
        if (decoder is AbstractAvroDirectDecoder) {
            var result: T? = null
            decoder.decodedCollectionSize = -1
            do {
                result = original.merge(decoder, result)
            } while (decoder.decodedCollectionSize > 0)
            return result!!
        }
        return original.deserialize(decoder)
    }

    override fun serialize(
        encoder: Encoder,
        value: T,
    ) {
        original.serialize(encoder, value)
    }
}