package com.github.avrokotlin.avro4k.schema

import kotlinx.serialization.descriptors.SerialDescriptor
import org.apache.avro.Schema

internal class ListVisitor(
    private val context: VisitorContext,
    private val onSchemaBuilt: (Schema) -> Unit,
) : SerialDescriptorListVisitor {
    private lateinit var itemSchema: Schema

    override fun visitListItem(
        listDescriptor: SerialDescriptor,
        itemElementIndex: Int,
    ): SerialDescriptorValueVisitor {
        return ValueVisitor(context) {
            itemSchema = it
        }
    }

    override fun endListVisit(descriptor: SerialDescriptor) {
        onSchemaBuilt(Schema.createArray(itemSchema))
    }
}