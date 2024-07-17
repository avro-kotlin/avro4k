package com.github.avrokotlin.avro4k.internal.schema

import com.github.avrokotlin.avro4k.internal.copy
import com.github.avrokotlin.avro4k.internal.isNamedSchema
import com.github.avrokotlin.avro4k.internal.jsonNode
import com.github.avrokotlin.avro4k.serializer.AvroSerializer
import com.github.avrokotlin.avro4k.serializer.ElementLocation
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import org.apache.avro.Schema

internal class InlineClassVisitor(
    private val context: VisitorContext,
    private val onSchemaBuilt: (Schema) -> Unit,
) : SerialDescriptorInlineClassVisitor {
    override fun visitInlineClassElement(
        inlineClassDescriptor: SerialDescriptor,
        inlineElementIndex: Int,
    ): SerialDescriptorValueVisitor {
        val inlinedElements = context.inlinedElements + ElementLocation(inlineClassDescriptor, inlineElementIndex)
        return ValueVisitor(context.copy(inlinedElements = inlinedElements)) { generatedSchema ->
            val annotations = InlineClassFieldAnnotations(inlineClassDescriptor)

            val props = annotations.props.toList()
            val schema =
                if (props.isNotEmpty()) {
                    if (generatedSchema.isNamedSchema()) {
                        throw SerializationException(
                            "The value class property '${inlineClassDescriptor.serialName}.${inlineClassDescriptor.getElementName(0)}' has " +
                                "forbidden additional properties $props for the named schema ${generatedSchema.fullName}. " +
                                "Please create your own serializer extending ${AvroSerializer::class.qualifiedName} to add properties to a named schema."
                        )
                    }
                    generatedSchema.copy(additionalProps = props.associate { it.key to it.jsonNode })
                } else {
                    generatedSchema
                }

            onSchemaBuilt(schema)
        }
    }
}