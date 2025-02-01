package com.github.avrokotlin.avro4k.serializer

import com.github.avrokotlin.avro4k.AvroDecoder
import com.github.avrokotlin.avro4k.AvroEncoder
import com.github.avrokotlin.avro4k.internal.SerializationMiddleware
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.builtins.ByteArraySerializer
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema
import java.util.*
import kotlin.io.encoding.Base64
import kotlin.io.encoding.ExperimentalEncodingApi
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.milliseconds


private val AvroStringSerialDescriptor: SerialDescriptor =
    SerialDescriptorWithAvroSchemaDelegate(String.serializer().descriptor) { context ->
        context.inlinedElements.firstNotNullOfOrNull {
            it.stringable?.createSchema() ?: it.fixed?.createSchema(it)
        } ?: Schema.create(Schema.Type.STRING)
    }


@ExperimentalSerializationApi
public val KotlinStdLibSerializersModule: SerializersModule =
    SerializersModule {
        contextual(Duration::class, KotlinDurationSerializer)
        contextual(ByteArray::class, AvroByteArraySerializer)
    }

internal val KotlinStdLibSerializationMiddleware = SerializationMiddleware(
    serializerOverrides = IdentityHashMap(
        mapOf(
            Duration.serializer() to KotlinDurationSerializer,
            ByteArraySerializer() to AvroByteArraySerializer
        )
    ),
    descriptorOverrides = IdentityHashMap(
        mapOf(
            Duration.serializer().descriptor to KotlinDurationSerializer.descriptor,
            String.serializer().descriptor to AvroStringSerialDescriptor,
            ByteArraySerializer().descriptor to AvroByteArraySerializer.descriptor
        )
    )
)

internal object AvroByteArraySerializer : AvroSerializer<ByteArray>(ByteArray::class.qualifiedName!!) {
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

internal object KotlinDurationSerializer : AvroSerializer<Duration>(Duration::class.qualifiedName!!) {
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
