package com.github.avrokotlin.avro4k.serializer

import com.github.avrokotlin.avro4k.internal.decoder.direct.AbstractAvroDirectDecoder
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.internal.AbstractCollectionSerializer

@OptIn(InternalSerializationApi::class)
internal class AvroCollectionSerializer<T>(private val original: AbstractCollectionSerializer<*, T, *>) :
    KSerializer<T> {
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