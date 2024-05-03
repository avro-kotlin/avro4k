package com.github.avrokotlin.avro4k.serializer

import com.github.avrokotlin.avro4k.decoder.AvroDecoder
import com.github.avrokotlin.avro4k.encoder.AvroEncoder
import kotlinx.serialization.KSerializer
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder

public abstract class AvroSerializer<T> : KSerializer<T> {
    final override fun serialize(
        encoder: Encoder,
        value: T,
    ) {
        if (encoder is AvroEncoder) {
            serializeAvro(encoder, value)
            return
        }
        serializeGeneric(encoder, value)
    }

    /**
     * This method is called when the serializer is used outside Avro serialization.
     * By default, it throws an exception.
     *
     * Implement it to provide a generic serialization logic with the standard [Encoder].
     */
    public open fun serializeGeneric(
        encoder: Encoder,
        value: T,
    ) {
        throw UnsupportedOperationException("The serializer ${this::class.qualifiedName} is not usable outside of Avro serialization.")
    }

    public abstract fun serializeAvro(
        encoder: AvroEncoder,
        value: T,
    )

    final override fun deserialize(decoder: Decoder): T {
        if (decoder !is AvroDecoder) {
            return deserializeGeneric(decoder)
        }
        return deserializeAvro(decoder)
    }

    public open fun deserializeGeneric(decoder: Decoder): T {
        throw UnsupportedOperationException("The serializer ${this::class.qualifiedName} is not usable outside of Avro serialization.")
    }

    public abstract fun deserializeAvro(decoder: AvroDecoder): T
}