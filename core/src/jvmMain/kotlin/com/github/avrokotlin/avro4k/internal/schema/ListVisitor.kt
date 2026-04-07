package com.github.avrokotlin.avro4k.internal.schema

import com.github.avrokotlin.avro4k.AvroSchema
import com.github.avrokotlin.avro4k.AvroSchema.ArraySchema
import kotlinx.serialization.descriptors.SerialDescriptor

internal class ListVisitor(
    private val context: VisitorContext,
    private val onSchemaBuilt: (AvroSchema) -> Unit,
) : SerialDescriptorListVisitor {
    private lateinit var itemSchema: AvroSchema

    override fun visitListItem(
        listDescriptor: SerialDescriptor,
        itemElementIndex: Int,
    ): SerialDescriptorValueVisitor {
        return ValueVisitor(context) {
            itemSchema = it
        }
    }

    override fun endListVisit(descriptor: SerialDescriptor) {
        onSchemaBuilt(ArraySchema(itemSchema))
    }
}