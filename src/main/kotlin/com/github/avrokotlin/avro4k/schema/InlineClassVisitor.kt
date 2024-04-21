package com.github.avrokotlin.avro4k.schema

import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor
import org.apache.avro.Schema

internal class InlineClassVisitor(
    override val context: VisitorContext,
    private val onSchemaBuilt: (Schema) -> Unit,
) : SerialDescriptorInlineClassVisitor, AvroVisitorContextAware {
    @OptIn(InternalSerializationApi::class)
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