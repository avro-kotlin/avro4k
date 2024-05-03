package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.schema.ensureTypeOf
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericFixed

internal class FixedEncoder(
    override val avro: Avro,
    arraySize: Int,
    private val schema: Schema,
    private val onEncoded: (GenericFixed) -> Unit,
) : AvroTaggedEncoder<Int>() {
    private val padSize = schema.fixedSize - arraySize
    private val output: ByteArray = ByteArray(schema.fixedSize)

    init {
        schema.ensureTypeOf(Schema.Type.FIXED)
        if (arraySize > schema.fixedSize) {
            throw SerializationException("Actual collection size $arraySize is greater than schema fixed size $schema")
        }
    }

    override fun encodeTaggedNull(tag: Int) {
        throw UnsupportedOperationException("nulls are not supported for schema type ${Schema.Type.FIXED}")
    }

    override fun endEncode(descriptor: SerialDescriptor) {
        onEncoded(GenericData.Fixed(schema, output))
    }

    override fun SerialDescriptor.getTag(index: Int) = padSize + index

    override val Int.writerSchema: Schema
        get() = this@FixedEncoder.schema

    override fun encodeTaggedByte(
        tag: Int,
        value: Byte,
    ) {
        output[tag] = value
    }
}