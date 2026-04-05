package com.github.avrokotlin.avro4k.internal.schema

import com.github.avrokotlin.avro4k.AvroSchema
import com.github.avrokotlin.avro4k.AvroSchema.UnionSchema
import com.github.avrokotlin.avro4k.ResolvedSchema
import com.github.avrokotlin.avro4k.internal.AvroSchemaGenerationException
import kotlinx.serialization.descriptors.SerialDescriptor

internal class PolymorphicVisitor(
    private val context: VisitorContext,
    private val onSchemaBuilt: (AvroSchema) -> Unit,
) : SerialDescriptorPolymorphicVisitor {
    private val possibleSchemas = mutableListOf<ResolvedSchema>()

    override fun visitPolymorphicFoundDescriptor(descriptor: SerialDescriptor): SerialDescriptorValueVisitor {
        return ValueVisitor(context) {
            possibleSchemas += it as ResolvedSchema
        }
    }

    override fun endPolymorphicVisit(descriptor: SerialDescriptor) {
        if (possibleSchemas.isEmpty()) {
            throw AvroSchemaGenerationException("Polymorphic descriptor '$descriptor' must have at least one possible schema")
        }
        if (possibleSchemas.size == 1) {
            // flatten the useless union schema
            onSchemaBuilt(possibleSchemas.first())
        } else {
            onSchemaBuilt(UnionSchema(possibleSchemas))
        }
    }
}