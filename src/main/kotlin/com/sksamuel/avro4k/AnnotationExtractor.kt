package com.sksamuel.avro4k

import kotlinx.serialization.SerialDescriptor

class AnnotationExtractor(private val annotations: List<Annotation>) {

  companion object {
    fun entity(descriptor: SerialDescriptor) = AnnotationExtractor(descriptor.getEntityAnnotations())
  }

  fun namespace(): String? = annotations.filterIsInstance<AvroNamespace>().firstOrNull()?.value
  fun name(): String? = annotations.filterIsInstance<AvroName>().firstOrNull()?.value
  fun doc(): String? = annotations.filterIsInstance<AvroDoc>().firstOrNull()?.value
  fun aliases(): List<String> = annotations.filterIsInstance<AvroAlias>().map { it.value }
}

