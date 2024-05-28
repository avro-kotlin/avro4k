package com.github.avrokotlin.avro4k.internal.decoder.direct

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.internal.IllegalIndexedAccessError
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor
import org.apache.avro.Schema

internal class AvroValueDirectDecoder(
    writerSchema: Schema,
    avro: Avro,
    binaryDecoder: org.apache.avro.io.Decoder,
) : AbstractAvroDirectDecoder(avro, binaryDecoder) {
    override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        throw IllegalIndexedAccessError()
    }

    @ExperimentalSerializationApi
    override var currentWriterSchema: Schema = writerSchema
}