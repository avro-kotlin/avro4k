package com.github.avrokotlin.avro4k

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor

@ExperimentalSerializationApi
class FieldNaming(private val name: String, annotations: List<Annotation>) {

   private val extractor = AnnotationExtractor(annotations)

   companion object {
      operator fun invoke(desc: SerialDescriptor, index: Int): FieldNaming = FieldNaming(
         desc.getElementName(index),
         desc.getElementAnnotations(index)
      )
   }


   /**
    *  Returns the avro name for the current element.
    *  Takes into account @AvroName.
    */
   fun name(): String = extractor.name() ?: name
}