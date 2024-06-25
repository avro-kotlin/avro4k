package com.github.avrokotlin.avro4k.internal.schema

import com.github.avrokotlin.avro4k.internal.copy
import com.github.avrokotlin.avro4k.internal.jsonNode
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
        val inlinedAnnotations =
            context.inlinedAnnotations.appendAnnotations(
                ValueAnnotations(
                    inlineClassDescriptor,
                    inlineElementIndex
                )
            )
        return ValueVisitor(context.copy(inlinedAnnotations = inlinedAnnotations)) { generatedSchema ->
            val annotations = InlineClassFieldAnnotations(inlineClassDescriptor)

            val props = annotations.props.toList()
            val schema =
                if (props.isNotEmpty()) {
                    generatedSchema.copy(additionalProps = props.associate { it.key to it.jsonNode })
                } else {
                    generatedSchema
                }

            onSchemaBuilt(schema)
        }
    }
}