package com.github.avrokotlin.avro4k

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.*
import kotlinx.serialization.modules.SerializersModule

@ExperimentalSerializationApi
fun SerialDescriptor.possibleSerializationSubclasses(serializersModule: SerializersModule): List<SerialDescriptor> {
    return when (this.kind) {
        StructureKind.CLASS, StructureKind.OBJECT -> listOf(this)
        PolymorphicKind.SEALED -> elementDescriptors.filter { it.kind == SerialKind.CONTEXTUAL }
            .flatMap { it.elementDescriptors }
            .flatMap { it.possibleSerializationSubclasses(serializersModule) }
        PolymorphicKind.OPEN ->
            serializersModule.getPolymorphicDescriptors(this)
                .flatMap { it.possibleSerializationSubclasses(serializersModule) }
        else -> throw UnsupportedOperationException("Can't get possible serialization subclasses for the SerialDescriptor of kind ${this.kind}.")
    }
}

