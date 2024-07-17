package com.github.avrokotlin.avro4k.internal.encoder.generic

import com.github.avrokotlin.avro4k.Avro
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.AbstractEncoder
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericFixed

internal class FixedGenericEncoder(
    private val avro: Avro,
    arraySize: Int,
    private val schema: Schema,
    private val onEncoded: (GenericFixed) -> Unit,
) : AbstractEncoder() {
    private val buffer = ByteArray(schema.fixedSize)
    private var pos = 0

    init {
        if (arraySize != schema.fixedSize) {
            throw SerializationException("Actual collection size $arraySize is greater than schema fixed size $schema")
        }
    }

    override val serializersModule: SerializersModule
        get() = avro.serializersModule

    override fun endStructure(descriptor: SerialDescriptor) {
        onEncoded(GenericData.Fixed(schema, buffer))
    }

    override fun encodeByte(value: Byte) {
        buffer[pos++] = value
    }
}