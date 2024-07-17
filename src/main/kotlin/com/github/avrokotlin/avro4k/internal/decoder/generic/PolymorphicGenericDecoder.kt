package com.github.avrokotlin.avro4k.internal.decoder.generic

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.internal.decoder.AbstractPolymorphicDecoder
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import org.apache.avro.Schema

internal class PolymorphicGenericDecoder(
    avro: Avro,
    descriptor: SerialDescriptor,
    schema: Schema,
    private val value: Any?,
) : AbstractPolymorphicDecoder(avro, descriptor, schema) {
    override fun tryFindSerialNameForUnion(
        namesAndAliasesToSerialName: Map<String, String>,
        schema: Schema,
    ): Pair<String, Schema>? {
        return schema.types.firstNotNullOfOrNull { tryFindSerialName(namesAndAliasesToSerialName, it) }
    }

    override fun newDecoder(chosenSchema: Schema): Decoder {
        return AvroValueGenericDecoder(avro, value, chosenSchema)
    }
}