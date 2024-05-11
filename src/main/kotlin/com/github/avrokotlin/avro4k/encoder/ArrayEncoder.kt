package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.internal.BadEncodedValueError
import kotlinx.serialization.descriptors.SerialDescriptor
import org.apache.avro.Schema
import org.apache.avro.generic.GenericArray
import org.apache.avro.generic.GenericData

internal class ArrayEncoder(
    override val avro: Avro,
    arraySize: Int,
    private val schema: Schema,
    private val onEncoded: (GenericArray<*>) -> Unit,
) : AbstractAvroEncoder() {
    private val values: Array<Any?> = Array(arraySize) { null }
    private var index = 0

    override lateinit var currentWriterSchema: Schema

    override fun encodeElement(
        descriptor: SerialDescriptor,
        index: Int,
    ): Boolean {
        super.encodeElement(descriptor, index)
        currentWriterSchema = schema.elementType
        return true
    }

    override fun endStructure(descriptor: SerialDescriptor) {
        onEncoded(GenericData.Array(schema, values.asList()))
    }

    override fun encodeValue(value: Any) {
        values[index++] = value
    }

    override fun encodeNull() {
        encodeResolvingUnion(
            { BadEncodedValueError(null, currentWriterSchema, Schema.Type.NULL) }
        ) {
            when (it.type) {
                Schema.Type.NULL -> {
                    { values[index++] = null }
                }

                else -> null
            }
        }
    }
}