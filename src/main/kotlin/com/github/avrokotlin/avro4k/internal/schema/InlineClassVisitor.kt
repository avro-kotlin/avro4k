package com.github.avrokotlin.avro4k.internal.schema

import com.github.avrokotlin.avro4k.internal.copy
import com.github.avrokotlin.avro4k.internal.jsonNode
import com.github.avrokotlin.avro4k.internal.overrideNamespace
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

            var schema = generatedSchema
            if (annotations.namespaceOverride != null) {
                schema = schema.overrideNamespace(annotations.namespaceOverride.value)
            }
            val props = annotations.props.toList()
            if (props.isNotEmpty()) {
                schema = schema.copy(additionalProps = props.associate { it.key to it.jsonNode })
            }

            onSchemaBuilt(schema)
        }
    }
}