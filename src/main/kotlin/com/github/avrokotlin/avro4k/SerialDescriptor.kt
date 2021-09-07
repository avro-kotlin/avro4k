package com.github.avrokotlin.avro4k

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.*
import kotlinx.serialization.modules.SerializersModule

@ExperimentalSerializationApi
fun SerialDescriptor.leavesOfSealedClasses(): List<SerialDescriptor> {
    return if (this.kind == PolymorphicKind.SEALED) {
        elementDescriptors.filter { it.kind == SerialKind.CONTEXTUAL }.flatMap { it.elementDescriptors }
            .flatMap { it.leavesOfSealedClasses() }
    } else {
        listOf(this)
    }
}

@ExperimentalSerializationApi
fun SerialDescriptor.possibleSerializationSubclasses(serializersModule: SerializersModule): List<SerialDescriptor> {
    return when (this.kind) {
        PolymorphicKind.SEALED -> this.leavesOfSealedClasses()
        PolymorphicKind.OPEN -> serializersModule.getPolymorphicDescriptors(this).sortedBy { it.serialName }
        else -> throw UnsupportedOperationException("Can't get possible serialization subclasses for the SerialDescriptor of kind ${this.kind}.")
    }
}

