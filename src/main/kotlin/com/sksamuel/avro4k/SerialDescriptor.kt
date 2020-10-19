package com.sksamuel.avro4k

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.PolymorphicKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.SerialKind
import kotlinx.serialization.descriptors.elementDescriptors

@ExperimentalSerializationApi
fun SerialDescriptor.leavesOfSealedClasses() : List<SerialDescriptor> {
   return if (this.kind == PolymorphicKind.SEALED) {
      elementDescriptors.filter {it.kind == SerialKind.CONTEXTUAL }.flatMap { it.elementDescriptors }.flatMap { it.leavesOfSealedClasses() }
   } else {
      listOf(this)
   }
}