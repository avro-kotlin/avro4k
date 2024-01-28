package com.github.avrokotlin.avro4k.serializer

import com.github.avrokotlin.avro4k.decoder.ExtendedDecoder
import com.github.avrokotlin.avro4k.encoder.ExtendedEncoder
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.Serializer
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.buildSerialDescriptor
import org.apache.avro.Schema
import org.apache.avro.util.Utf8
import java.net.URL

@OptIn(ExperimentalSerializationApi::class)
@Serializer(forClass = URL::class)
class URLSerializer : AvroSerializer<URL>() {
    @OptIn(InternalSerializationApi::class)
    override val descriptor = buildSerialDescriptor(URL::class.qualifiedName!!, PrimitiveKind.STRING)

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
            is Utf8 -> URL(v.toString())
            is String -> URL(v)
            null -> throw SerializationException("Cannot decode <null> as URL")
            else -> throw SerializationException("Unsupported URL type [$v : ${v::class.qualifiedName}]")
        }
    }
}