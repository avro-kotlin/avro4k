package com.github.avrokotlin.avro4k.schema

import kotlinx.serialization.descriptors.SerialDescriptor
import org.apache.avro.Schema

internal class PolymorphicVisitor(
    override val context: VisitorContext,
    private val onSchemaBuilt: (Schema) -> Unit,
) : SerialDescriptorPolymorphicVisitor, AvroVisitorContextAware {
    private val possibleSchemas = mutableListOf<Schema>()

    override fun visitPolymorphicFoundDescriptor(descriptor: SerialDescriptor): SerialDescriptorValueVisitor {
        return ValueVisitor(context) {
            possibleSchemas += it
        }
    }

    override fun endPolymorphicVisit(descriptor: SerialDescriptor) {
        if (possibleSchemas.isEmpty()) {
            throw AvroSchemaGenerationException("Polymorphic descriptor must have at least one possible schema")
        }
        if (possibleSchemas.size == 1) {
            // flatten the useless union schema
            onSchemaBuilt(possibleSchemas.first())
        } else {
            onSchemaBuilt(Schema.createUnion(possibleSchemas))
        }
    }
}