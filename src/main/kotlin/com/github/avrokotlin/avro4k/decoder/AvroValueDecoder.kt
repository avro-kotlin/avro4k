package com.github.avrokotlin.avro4k.decoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.internal.DecodedNullError
import com.github.avrokotlin.avro4k.internal.IllegalIndexedAccessError
import kotlinx.serialization.descriptors.SerialDescriptor
import org.apache.avro.Schema

internal class AvroValueDecoder(
    override val avro: Avro,
    val value: Any?,
    val writerSchema: Schema,
) : AvroTaggedDecoder<Schema>() {
    init {
        pushTag(writerSchema)
    }

    override val Schema.writerSchema: Schema
        get() = this@AvroValueDecoder.writerSchema

    override fun decodeTaggedNotNullMark(tag: Schema) = value != null

    override fun decodeTaggedValue(tag: Schema) = value ?: throw DecodedNullError()

    override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        throw IllegalIndexedAccessError()
    }

    override fun SerialDescriptor.getTag(index: Int) = this@AvroValueDecoder.writerSchema
}