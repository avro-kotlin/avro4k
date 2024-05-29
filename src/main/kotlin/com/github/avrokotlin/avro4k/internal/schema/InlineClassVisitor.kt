package com.github.avrokotlin.avro4k.internal.schema

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
        return ValueVisitor(context.copy(inlinedAnnotations = inlinedAnnotations)) {
            val annotations = InlineClassFieldAnnotations(inlineClassDescriptor, inlineElementIndex)
            if (annotations.namespaceOverride != null) {
                onSchemaBuilt(it.overrideNamespace(annotations.namespaceOverride.value))
            } else {
                onSchemaBuilt(it)
            }
        }
    }
}