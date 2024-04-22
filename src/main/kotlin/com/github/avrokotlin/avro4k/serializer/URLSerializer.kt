package com.github.avrokotlin.avro4k.serializer

import com.github.avrokotlin.avro4k.decoder.ExtendedDecoder
import com.github.avrokotlin.avro4k.encoder.ExtendedEncoder
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import org.apache.avro.Schema
import java.net.URL

object URLSerializer : AvroSerializer<URL>() {
    override val descriptor = PrimitiveSerialDescriptor(URL::class.qualifiedName!!, PrimitiveKind.STRING)

    override fun encodeAvroValue(
        schema: Schema,
        encoder: ExtendedEncoder,
        obj: URL,
    ) {
        encoder.encodeString(obj.toString())
    }

    override fun decodeAvroValue(
        schema: Schema,
        decoder: ExtendedDecoder,
    ): URL {
        return when (val v = decoder.decodeAny()) {
            is CharSequence -> URL(v.toString())
            null -> throw SerializationException("Cannot decode <null> as URL")
            else -> throw SerializationException("Unsupported URL type [$v : ${v::class.qualifiedName}]")
        }
    }
}