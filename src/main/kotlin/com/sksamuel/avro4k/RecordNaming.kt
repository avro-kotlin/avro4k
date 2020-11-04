package com.sksamuel.avro4k

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor

@ExperimentalSerializationApi
class RecordNaming(name: String, annotations: List<Annotation>) {

   companion object {
      operator fun invoke(descriptor: SerialDescriptor): RecordNaming = RecordNaming(
         if (descriptor.isNullable) descriptor.serialName.removeSuffix("?") else descriptor.serialName,
         descriptor.annotations
      )

      operator fun invoke(descriptor: SerialDescriptor, index: Int): RecordNaming = RecordNaming(
         descriptor.getElementName(index),
         descriptor.getElementAnnotations(index)
      )
   }

   private val annotations = AnnotationExtractor(annotations)

   private val className = name
      .replace(".<init>", "")
      .replace(".<anonymous>", "")

   private val defaultNamespace = className.split('.').dropLast(1).joinToString(".")

   // the name of the scala class without type parameters.
   // Eg, List[Int] would be List.
   private val erasedName = className.split('.').last()

   // the name of the scala class with type parameters encoded,
   // Eg, List[Int] would be `List__Int`
   // Eg, Type[A, B] would be `Type__A_B`
   // this method must also take into account @AvroName on the classes used as type arguments
//  private val genericName = {
//    if (typeInfo.typeArguments.isEmpty) {
//      erasedName
//    } else {
//      val targs = typeInfo.typeArguments.map { typeArgInfo => NameExtractor(typeArgInfo).name }.mkString("_")
//      typeInfo.short + "__" + targs
//    }
//  }

   /**
    * Returns the namespace for this type to be used when creating
    * an avro record. This method takes into account @AvroNamespace.
    */
   fun namespace(): String = annotations.namespace() ?: defaultNamespace

   /**
    * Returns the record name for this type to be used when creating
    * an avro record. This method takes into account type parameters and
    * annotations.
    *
    * The general format for a record name is `resolved-name__typea_typeb_typec`.
    * That is a double underscore delimits the resolved name from the start of the
    * type parameters and then each type parameter is delimited by a single underscore.
    *
    * The resolved name is the class name with any annotations applied, such
    * as @AvroName or @AvroNamespace, or @AvroErasedName, which, if present,
    * means the type parameters will not be included in the final name.
    */
   fun name(): String = annotations.name() ?: erasedName

   /**
    * Returns the qualified name for this type with any annotation applied.
    * It should be unique per schema as it is used to retrieve
    * already resolved schemas in recursive cases
    */
   fun qualifiedName(): String = "${namespace()}.${name()}"
}
