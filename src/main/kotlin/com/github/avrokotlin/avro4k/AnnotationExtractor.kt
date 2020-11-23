package com.github.avrokotlin.avro4k

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor
@ExperimentalSerializationApi
class AnnotationExtractor(private val annotations: List<Annotation>) {

   companion object {
      fun entity(descriptor: SerialDescriptor) = _root_ide_package_.com.github.avrokotlin.avro4k.AnnotationExtractor(
         descriptor.annotations)

      operator fun invoke(descriptor: SerialDescriptor, index: Int): _root_ide_package_.com.github.avrokotlin.avro4k.AnnotationExtractor =
         _root_ide_package_.com.github.avrokotlin.avro4k.AnnotationExtractor(descriptor.getElementAnnotations(index))
   }

   fun fixed(): Int? = annotations.filterIsInstance<_root_ide_package_.com.github.avrokotlin.avro4k.AvroFixed>().firstOrNull()?.size
   fun scalePrecision(): Pair<Int, Int>? = annotations.filterIsInstance<_root_ide_package_.com.github.avrokotlin.avro4k.ScalePrecision>().firstOrNull()?.let { it.scale to it.precision }
   fun namespace(): String? = annotations.filterIsInstance<_root_ide_package_.com.github.avrokotlin.avro4k.AvroNamespace>().firstOrNull()?.value
   fun name(): String? = annotations.filterIsInstance<_root_ide_package_.com.github.avrokotlin.avro4k.AvroName>().firstOrNull()?.value
   fun valueType(): Boolean = annotations.filterIsInstance<_root_ide_package_.com.github.avrokotlin.avro4k.AvroInline>().isNotEmpty()
   fun doc(): String? = annotations.filterIsInstance<_root_ide_package_.com.github.avrokotlin.avro4k.AvroDoc>().firstOrNull()?.value
   fun aliases(): List<String> =
      if(annotations.any { it is _root_ide_package_.com.github.avrokotlin.avro4k.AvroAlias }){
         annotations.filterIsInstance<_root_ide_package_.com.github.avrokotlin.avro4k.AvroAlias>().map { it.value }
      }else{
         annotations.filterIsInstance<_root_ide_package_.com.github.avrokotlin.avro4k.AvroAliases>().flatMap { it.value.toList() }
      }
   fun props(): List<Pair<String, String>> = annotations.filterIsInstance<_root_ide_package_.com.github.avrokotlin.avro4k.AvroProp>().map { it.key to it.value }
   fun default(): String? = annotations.filterIsInstance<_root_ide_package_.com.github.avrokotlin.avro4k.AvroDefault>().firstOrNull()?.value
}

