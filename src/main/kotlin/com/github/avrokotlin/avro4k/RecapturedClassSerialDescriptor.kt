package com.github.avrokotlin.avro4k

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlin.reflect.KClass

class RecapturedClassSerialDescriptor(private val delegate: SerialDescriptor, val capturedKClass: KClass<*>): SerialDescriptor {
    @ExperimentalSerializationApi
    override val elementsCount = delegate.elementsCount

    @ExperimentalSerializationApi
    override val kind = delegate.kind

    @ExperimentalSerializationApi
    override val serialName = delegate.serialName

    @ExperimentalSerializationApi
    override fun getElementAnnotations(index: Int) = delegate.getElementAnnotations(index)

    @ExperimentalSerializationApi
    override fun getElementDescriptor(index: Int) = RecapturedClassSerialDescriptor(delegate.getElementDescriptor(index), capturedKClass)

    @ExperimentalSerializationApi
    override fun getElementIndex(name: String) = delegate.getElementIndex(name)

    @ExperimentalSerializationApi
    override fun getElementName(index: Int) = delegate.getElementName(index)

    @ExperimentalSerializationApi
    override fun isElementOptional(index: Int) = delegate.isElementOptional(index)
}