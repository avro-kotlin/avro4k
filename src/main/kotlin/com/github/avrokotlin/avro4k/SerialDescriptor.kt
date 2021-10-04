@file:Suppress("UNCHECKED_CAST")

package com.github.avrokotlin.avro4k

import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.descriptors.*
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.internal.AbstractPolymorphicSerializer
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.serializer
import kotlin.reflect.full.starProjectedType

@OptIn(InternalSerializationApi::class)
fun <T : Any> AbstractPolymorphicSerializer<T>.findPolymorphicSerializerInclSealedInterfaceOrNull(
    decoder: CompositeDecoder,
    klassName: String?
): DeserializationStrategy<out T>? {
    return decoder.serializersModule.getPolymorphicInclSealedInterfaces(baseClass, klassName)
}

@ExperimentalSerializationApi
fun SerialDescriptor.possibleSerializationSubclasses(serializersModule: SerializersModule): List<SerialDescriptor> {
    return when (this.kind) {
        StructureKind.CLASS, StructureKind.OBJECT -> listOf(this)
        PolymorphicKind.SEALED -> elementDescriptors.filter { it.kind == SerialKind.CONTEXTUAL }
            .flatMap { it.elementDescriptors }
            .flatMap { it.possibleSerializationSubclasses(serializersModule) }
        PolymorphicKind.OPEN -> {
            val capturedClass = this.capturedKClass
            val descriptors = if (capturedClass?.isSealed == true) {
                //A sealed interface will have the kind PolymorphicKind.OPEN, although it is sealed. Retrieve all the possible direct subclasses of the interface
                capturedClass.sealedSubclasses.map { serializersModule.serializer(it.starProjectedType) }
                    .map { it.descriptor }
                    .flatMap { it.possibleSerializationSubclasses(serializersModule) }
            } else {
                serializersModule.getPolymorphicDescriptors(this)
                    .flatMap { it.possibleSerializationSubclasses(serializersModule) }
            }
            // descriptors may exist more than once due to diamond inheritance so we need to filter duplicates out
            descriptors.distinct()
        }
        else -> throw UnsupportedOperationException("Can't get possible serialization subclasses for the SerialDescriptor of kind ${this.kind}.")
    }
}

