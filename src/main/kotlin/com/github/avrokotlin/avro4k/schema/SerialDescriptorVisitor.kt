package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.internal.getNonNullContextualDescriptor
import com.github.avrokotlin.avro4k.internal.possibleSerializationSubclasses
import kotlinx.serialization.descriptors.PolymorphicKind
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.SerialKind
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.modules.SerializersModule

internal interface SerialDescriptorValueVisitor {
    val serializersModule: SerializersModule

    /**
     * Called when the [descriptor]'s kind is a [PrimitiveKind].
     */
    fun visitPrimitive(
        descriptor: SerialDescriptor,
        kind: PrimitiveKind,
    )

    /**
     * Called when the [descriptor]'s kind is an [SerialKind.ENUM].
     */
    fun visitEnum(descriptor: SerialDescriptor)

    /**
     * Called when the [descriptor]'s kind is an [StructureKind.OBJECT].
     */
    fun visitObject(descriptor: SerialDescriptor)

    /**
     * Called when the [descriptor]'s kind is a [PolymorphicKind].
     * @return null if we don't want to visit the polymorphic type
     */
    fun visitPolymorphic(
        descriptor: SerialDescriptor,
        kind: PolymorphicKind,
    ): SerialDescriptorPolymorphicVisitor?

    /**
     * Called when the [descriptor]'s kind is a [StructureKind.CLASS].
     * Note that when the [descriptor] is an inline class, [visitInlineClass] is called instead.
     * @return null if we don't want to visit the class
     */
    fun visitClass(descriptor: SerialDescriptor): SerialDescriptorClassVisitor?

    /**
     * Called when the [descriptor]'s kind is a [StructureKind.LIST].
     * @return null if we don't want to visit the list
     */
    fun visitList(descriptor: SerialDescriptor): SerialDescriptorListVisitor?

    /**
     * Called when the [descriptor]'s kind is a [StructureKind.MAP].
     * @return null if we don't want to visit the map
     */
    fun visitMap(descriptor: SerialDescriptor): SerialDescriptorMapVisitor?

    /**
     * Called when the [descriptor] is about a value class (e.g. its kind is a [StructureKind.CLASS] and [SerialDescriptor.isInline] is true).
     * @return null if we don't want to visit the inline class
     */
    fun visitInlineClass(descriptor: SerialDescriptor): SerialDescriptorInlineClassVisitor?

    fun visitValue(descriptor: SerialDescriptor) {
        if (descriptor.isInline) {
            visitInlineClass(descriptor)?.apply {
                visitInlineClassElement(descriptor, 0)?.visitValue(descriptor.getElementDescriptor(0))
            }
        } else {
            when (descriptor.kind) {
                is PrimitiveKind -> visitPrimitive(descriptor, descriptor.kind as PrimitiveKind)
                SerialKind.ENUM -> visitEnum(descriptor)
                SerialKind.CONTEXTUAL -> visitValue(descriptor.getNonNullContextualDescriptor(serializersModule))
                StructureKind.CLASS ->
                    visitClass(descriptor)?.apply {
                        for (elementIndex in (0 until descriptor.elementsCount)) {
                            visitClassElement(descriptor, elementIndex)?.visitValue(descriptor.getElementDescriptor(elementIndex))
                        }
                    }?.endClassVisit(descriptor)

                StructureKind.LIST ->
                    visitList(descriptor)?.apply {
                        visitListItem(descriptor, 0)?.visitValue(descriptor.getElementDescriptor(0))
                    }?.endListVisit(descriptor)

                StructureKind.MAP ->
                    visitMap(descriptor)?.apply {
                        visitMapKey(descriptor, 0)?.visitValue(descriptor.getElementDescriptor(0))
                        visitMapValue(descriptor, 1)?.visitValue(descriptor.getElementDescriptor(1))
                    }?.endMapVisit(descriptor)

                is PolymorphicKind ->
                    visitPolymorphic(descriptor, descriptor.kind as PolymorphicKind)?.apply {
                        descriptor.possibleSerializationSubclasses(serializersModule).sortedBy { it.serialName }.forEach { implementationDescriptor ->
                            visitPolymorphicFoundDescriptor(implementationDescriptor)?.visitValue(implementationDescriptor)
                        }
                    }?.endPolymorphicVisit(descriptor)

                StructureKind.OBJECT -> visitObject(descriptor)
            }
        }
    }
}

internal interface SerialDescriptorMapVisitor {
    /**
     * @return null if we don't want to visit the map key
     */
    fun visitMapKey(
        mapDescriptor: SerialDescriptor,
        keyElementIndex: Int,
    ): SerialDescriptorValueVisitor?

    /**
     * @return null if we don't want to visit the map value
     */
    fun visitMapValue(
        mapDescriptor: SerialDescriptor,
        valueElementIndex: Int,
    ): SerialDescriptorValueVisitor?

    fun endMapVisit(descriptor: SerialDescriptor)
}

internal interface SerialDescriptorListVisitor {
    /**
     * @return null if we don't want to visit the list item
     */
    fun visitListItem(
        listDescriptor: SerialDescriptor,
        itemElementIndex: Int,
    ): SerialDescriptorValueVisitor?

    fun endListVisit(descriptor: SerialDescriptor)
}

internal interface SerialDescriptorPolymorphicVisitor {
    /**
     * @return null if we don't want to visit the found polymorphic descriptor
     */
    fun visitPolymorphicFoundDescriptor(descriptor: SerialDescriptor): SerialDescriptorValueVisitor?

    fun endPolymorphicVisit(descriptor: SerialDescriptor)
}

internal interface SerialDescriptorClassVisitor {
    /**
     * @return null if we don't want to visit the class element
     */
    fun visitClassElement(
        descriptor: SerialDescriptor,
        elementIndex: Int,
    ): SerialDescriptorValueVisitor?

    fun endClassVisit(descriptor: SerialDescriptor)
}

internal interface SerialDescriptorInlineClassVisitor {
    /**
     * @return null if we don't want to visit the inline class element
     */
    fun visitInlineClassElement(
        inlineClassDescriptor: SerialDescriptor,
        inlineElementIndex: Int,
    ): SerialDescriptorValueVisitor?
}