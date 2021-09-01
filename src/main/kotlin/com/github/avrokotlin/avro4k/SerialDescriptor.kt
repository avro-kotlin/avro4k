package com.github.avrokotlin.avro4k

import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.PolymorphicKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.SerialKind
import kotlinx.serialization.descriptors.capturedKClass
import kotlinx.serialization.descriptors.elementDescriptors
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.SerializersModuleCollector
import kotlin.reflect.KClass

@ExperimentalSerializationApi
fun SerialDescriptor.leavesOfSealedClasses() : List<SerialDescriptor> {
   return if (this.kind == PolymorphicKind.SEALED) {
      elementDescriptors.filter {it.kind == SerialKind.CONTEXTUAL }.flatMap { it.elementDescriptors }.flatMap { it.leavesOfSealedClasses() }
   } else {
      listOf(this)
   }
}

@ExperimentalSerializationApi
fun SerialDescriptor.explicitlyNamedSubclassesFrom(serializersModule: SerializersModule): List<SerialDescriptor> {
   return if (this.kind == PolymorphicKind.OPEN) {
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
      captured.filter { it.first == this.capturedKClass }.map { it.second.descriptor }.sortedBy { it.serialName }
   } else {
      listOf(this)
   }
}