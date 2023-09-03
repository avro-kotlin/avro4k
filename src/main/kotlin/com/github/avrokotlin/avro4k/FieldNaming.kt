package com.github.avrokotlin.avro4k

import kotlinx.serialization.ExperimentalSerializationApi

@ExperimentalSerializationApi
class FieldNaming(private val name: String, annotations: List<Annotation>) {

   private val extractor = AnnotationExtractor(annotations)


   /**
    *  Returns the avro name for the current element.
    *  Takes into account @AvroName.
    */
   fun name(): String = extractor.name() ?: name
}