package com.github.avrokotlin.avro4k.schema

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.descriptors.PolymorphicKind
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.SerialKind
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.descriptors.capturedKClass
import kotlinx.serialization.descriptors.elementDescriptors
import kotlinx.serialization.descriptors.getContextualDescriptor
import kotlinx.serialization.descriptors.getPolymorphicDescriptors
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.serializerOrNull

@ExperimentalSerializationApi
interface SerialDescriptorValueVisitor {
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

@ExperimentalSerializationApi
interface SerialDescriptorMapVisitor {
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

@ExperimentalSerializationApi
interface SerialDescriptorListVisitor {
    /**
     * @return null if we don't want to visit the list item
     */
    fun visitListItem(
        listDescriptor: SerialDescriptor,
        itemElementIndex: Int,
    ): SerialDescriptorValueVisitor?

    fun endListVisit(descriptor: SerialDescriptor)
}

@ExperimentalSerializationApi
interface SerialDescriptorPolymorphicVisitor {
    /**
     * @return null if we don't want to visit the found polymorphic descriptor
     */
    fun visitPolymorphicFoundDescriptor(descriptor: SerialDescriptor): SerialDescriptorValueVisitor?

    fun endPolymorphicVisit(descriptor: SerialDescriptor)
}

@ExperimentalSerializationApi
interface SerialDescriptorClassVisitor {
    /**
     * @return null if we don't want to visit the class element
     */
    fun visitClassElement(
        descriptor: SerialDescriptor,
        elementIndex: Int,
    ): SerialDescriptorValueVisitor?

    fun endClassVisit(descriptor: SerialDescriptor)
}

@ExperimentalSerializationApi
interface SerialDescriptorInlineClassVisitor {
    /**
     * @return null if we don't want to visit the inline class element
     */
    fun visitInlineClassElement(
        inlineClassDescriptor: SerialDescriptor,
        inlineElementIndex: Int,
    ): SerialDescriptorValueVisitor?
}

@ExperimentalSerializationApi
@OptIn(InternalSerializationApi::class)
private fun SerialDescriptor.getNonNullContextualDescriptor(serializersModule: SerializersModule) =
    requireNotNull(serializersModule.getContextualDescriptor(this) ?: this.capturedKClass?.serializerOrNull()?.descriptor) {
        "No descriptor found in serialization context for $this"
    }

@ExperimentalSerializationApi
internal fun SerialDescriptor.possibleSerializationSubclasses(serializersModule: SerializersModule): Sequence<SerialDescriptor> {
    return when (this.kind) {
        PolymorphicKind.SEALED ->
            elementDescriptors.asSequence()
                .filter { it.kind == SerialKind.CONTEXTUAL }
                .flatMap { it.elementDescriptors }
                .flatMap { it.possibleSerializationSubclasses(serializersModule) }

        PolymorphicKind.OPEN ->
            serializersModule.getPolymorphicDescriptors(this@possibleSerializationSubclasses).asSequence()
                .flatMap { it.possibleSerializationSubclasses(serializersModule) }

        SerialKind.CONTEXTUAL -> sequenceOf(getNonNullContextualDescriptor(serializersModule))

        else -> sequenceOf(this)
    }
}