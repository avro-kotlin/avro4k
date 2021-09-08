package com.github.avrokotlin.avro4k

import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SealedClassSerializer
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.*
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.SerializersModuleCollector
import kotlin.reflect.KClass

@ExperimentalSerializationApi
fun SerialDescriptor.leavesOfSealedClasses(): List<SerialDescriptor> {
    return if (this.kind == PolymorphicKind.SEALED) {
        elementDescriptors.filter { it.kind == SerialKind.CONTEXTUAL }.flatMap { it.elementDescriptors }
            .flatMap { it.leavesOfSealedClasses() }
    } else {
        listOf(this)
    }
}

@OptIn(InternalSerializationApi::class)
@ExperimentalSerializationApi
fun SerialDescriptor.possibleSerializationSubclasses(serializersModule: SerializersModule): List<SerialDescriptor> {
    return when (this.kind) {
        PolymorphicKind.SEALED -> this.leavesOfSealedClasses()
        PolymorphicKind.OPEN -> {
            val captured = mutableListOf<Pair<KClass<*>, KSerializer<*>>>()
            val collector = object : SerializersModuleCollector {
                override fun <T : Any> contextual(kClass: KClass<T>, provider: (typeArgumentsSerializers: List<KSerializer<*>>) -> KSerializer<*>) {
                }

                override fun <Base : Any, Sub : Base> polymorphic(baseClass: KClass<Base>, actualClass: KClass<Sub>, actualSerializer: KSerializer<Sub>) {
                    captured.add(baseClass to actualSerializer)
                }

                override fun <Base : Any> polymorphicDefault(baseClass: KClass<Base>, defaultSerializerProvider: (className: String?) -> DeserializationStrategy<out Base>?) {
                    throw SerializationException("Polymorphic defaults are not supported in avro4k. Error whilst describing: ${capturedKClass?.simpleName}")
                }
            }
            serializersModule.dumpTo(collector)
            val serializers = captured.filter { it.first == this.capturedKClass }.map { it.second }
            serializers.map { when(it) {
                is SealedClassSerializer -> RecapturedClassSerialDescriptor(it.descriptor, it.baseClass)
                else -> it.descriptor
            } }.sortedBy { it.serialName }
        }
        else -> throw UnsupportedOperationException("Can't get possible serialization subclasses for the SerialDescriptor of kind ${this.kind}.")
    }
}

