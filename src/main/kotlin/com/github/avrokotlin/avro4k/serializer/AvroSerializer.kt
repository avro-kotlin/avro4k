package com.github.avrokotlin.avro4k.serializer

import com.github.avrokotlin.avro4k.decoder.ExtendedDecoder
import com.github.avrokotlin.avro4k.decoder.FieldDecoder
import com.github.avrokotlin.avro4k.encoder.ExtendedEncoder
import com.github.avrokotlin.avro4k.encoder.FieldEncoder
import com.github.avrokotlin.avro4k.schema.extractNonNull
import kotlinx.serialization.KSerializer
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import org.apache.avro.Schema

abstract class AvroSerializer<T> : KSerializer<T> {
    final override fun serialize(
        encoder: Encoder,
        value: T,
    ) {
        val schema =
            (encoder as FieldEncoder).fieldSchema().let {
                if (!this.descriptor.isNullable && it.isNullable) {
                    it.extractNonNull()
                } else {
                    it
                }
            }
        encodeAvroValue(schema, encoder, value)
    }

    final override fun deserialize(decoder: Decoder): T {
        val schema =
            (decoder as FieldDecoder).fieldSchema().let {
                if (!this.descriptor.isNullable && it.isNullable) {
                    it.extractNonNull()
                } else {
                    it
                }
            }
        return decodeAvroValue(schema, decoder)
    }

    abstract fun encodeAvroValue(
        schema: Schema,
        encoder: ExtendedEncoder,
        obj: T,
    )

    abstract fun decodeAvroValue(
        schema: Schema,
        decoder: ExtendedDecoder,
    ): T
}