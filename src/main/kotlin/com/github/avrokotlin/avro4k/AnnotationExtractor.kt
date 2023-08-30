package com.github.avrokotlin.avro4k

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor

@ExperimentalSerializationApi
class AnnotationExtractor(private val annotations: List<Annotation>) {

   companion object {
      fun entity(descriptor: SerialDescriptor) = AnnotationExtractor(
         descriptor.annotations)

      operator fun invoke(descriptor: SerialDescriptor, index: Int): AnnotationExtractor =
         AnnotationExtractor(descriptor.getElementAnnotations(index))
   }

   fun fixed(): Int? = annotations.filterIsInstance<AvroFixed>().firstOrNull()?.size
   fun scalePrecision(): Pair<Int, Int>? = annotations.filterIsInstance<ScalePrecision>().firstOrNull()?.let { it.scale to it.precision }
   fun namespace(): String? = annotations.filterIsInstance<AvroNamespace>().firstOrNull()?.value
   fun name(): String? = annotations.filterIsInstance<AvroName>().firstOrNull()?.value
   fun valueType(): Boolean = annotations.filterIsInstance<AvroInline>().isNotEmpty()
   fun doc(): String? = annotations.filterIsInstance<AvroDoc>().firstOrNull()?.value
   fun aliases(): List<String> = (annotations.firstNotNullOfOrNull { it as? AvroAlias }?.value ?: emptyArray()).asList() + (annotations.firstNotNullOfOrNull {it as? AvroAliases}?.value ?: emptyArray())
   fun props(): List<Pair<String, String>> = annotations.filterIsInstance<AvroProp>().map { it.key to it.value }
   fun jsonProps(): List<Pair<String, String>> = annotations.filterIsInstance<AvroJsonProp>().map { it.key to it.jsonValue }
   fun default(): String? = annotations.filterIsInstance<AvroDefault>().firstOrNull()?.value
   fun enumDefault(): String? = annotations.filterIsInstance<AvroEnumDefault>().firstOrNull()?.value
}
