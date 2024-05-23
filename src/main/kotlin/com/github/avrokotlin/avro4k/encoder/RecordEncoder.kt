package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.ListRecord
import kotlinx.serialization.descriptors.SerialDescriptor
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

internal class RecordEncoder(
    override val avro: Avro,
    descriptor: SerialDescriptor,
    private val schema: Schema,
    private val onEncoded: (GenericRecord) -> Unit,
) : AbstractAvroEncoder() {
    private val fieldValues: Array<Any?> = Array(schema.fields.size) { null }

    // from descriptor element index to schema field
    private val fields = avro.recordResolver.resolveFields(schema, descriptor)
    private lateinit var currentField: Schema.Field

    override lateinit var currentWriterSchema: Schema

    override fun encodeElement(
        descriptor: SerialDescriptor,
        index: Int,
    ): Boolean {
        super.encodeElement(descriptor, index)
        // Skip data class fields that are not present in the schema
        val writerFieldIndex = fields[index]?.writerFieldIndex
        if (writerFieldIndex != null) {
            val field = schema.fields[writerFieldIndex]
            currentField = field
            currentWriterSchema = field.schema()
            return true
        }
        return false
    }

    override fun endStructure(descriptor: SerialDescriptor) {
        onEncoded(ListRecord(schema, fieldValues.asList()))
    }

    override fun encodeValue(value: Any) {
        fieldValues[currentField.pos()] = value
    }

    override fun encodeNull() {
        fieldValues[currentField.pos()] = null
    }
}