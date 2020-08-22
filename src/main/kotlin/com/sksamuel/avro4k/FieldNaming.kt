package com.sksamuel.avro4k

import kotlinx.serialization.SerialDescriptor

class FieldNaming(private val name: String, private val annotations: List<Annotation>) {

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