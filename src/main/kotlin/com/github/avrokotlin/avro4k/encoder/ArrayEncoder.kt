package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.internal.ensureTypeOf
import kotlinx.serialization.descriptors.SerialDescriptor
import org.apache.avro.Schema
import org.apache.avro.generic.GenericArray
import org.apache.avro.generic.GenericData

internal class ArrayEncoder(
    override val avro: Avro,
    arraySize: Int,
    private val schema: Schema,
    private val onEncoded: (GenericArray<*>) -> Unit,
) : AvroTaggedEncoder<Int>() {
    init {
        schema.ensureTypeOf(Schema.Type.ARRAY)
    }

    private val values: Array<Any?> = Array(arraySize) { null }

    override fun endEncode(descriptor: SerialDescriptor) {
        onEncoded(GenericData.Array(schema, values.asList()))
    }

    override fun SerialDescriptor.getTag(index: Int) = index

    override val Int.writerSchema: Schema
        get() = this@ArrayEncoder.schema.elementType

    override fun encodeTaggedValue(
        tag: Int,
        value: Any,
    ) {
        values[tag] = value
    }

    override fun encodeTaggedNull(tag: Int) {
        require(tag.writerSchema.isNullable)
        values[tag] = null
    }
}