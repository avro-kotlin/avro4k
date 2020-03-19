package com.sksamuel.avro4k

import kotlinx.serialization.PolymorphicKind
import kotlinx.serialization.SerialDescriptor
import kotlinx.serialization.UnionKind
import kotlinx.serialization.elementDescriptors

fun SerialDescriptor.leafsOfSealedClasses() : List<SerialDescriptor> {
   return if (this.kind == PolymorphicKind.SEALED) {
      elementDescriptors().filter {it.kind == UnionKind.CONTEXTUAL }.flatMap { it.elementDescriptors() }.flatMap { it.leafsOfSealedClasses() }
   } else {
      listOf(this)
   }
}