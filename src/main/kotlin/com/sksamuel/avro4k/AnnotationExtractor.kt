package com.sksamuel.avro4k

import kotlinx.serialization.SerialDescriptor

class AnnotationExtractor(private val annotations: List<Annotation>) {

   companion object {
      fun entity(descriptor: SerialDescriptor) = AnnotationExtractor(descriptor.getEntityAnnotations())
   }

   fun fixed(): Int? = annotations.filterIsInstance<AvroFixed>().firstOrNull()?.size
   fun scalePrecision(): Pair<Int, Int>? = annotations.filterIsInstance<ScalePrecision>().firstOrNull()?.let { it.scale to it.precision }
   fun namespace(): String? = annotations.filterIsInstance<AvroNamespace>().firstOrNull()?.value
   fun name(): String? = annotations.filterIsInstance<AvroName>().firstOrNull()?.value
   fun doc(): String? = annotations.filterIsInstance<AvroDoc>().firstOrNull()?.value
   fun aliases(): List<String> = annotations.filterIsInstance<AvroAlias>().map { it.value }
   fun props(): List<Pair<String, String>> = annotations.filterIsInstance<AvroProp>().map { it.key to it.value }
}

