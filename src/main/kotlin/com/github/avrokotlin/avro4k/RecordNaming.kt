package com.github.avrokotlin.avro4k

import com.github.avrokotlin.avro4k.schema.DefaultNamingStrategy
import com.github.avrokotlin.avro4k.schema.NamingStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor

@ExperimentalSerializationApi
data class RecordNaming internal constructor(
   /**
    * The record name for this type to be used when creating
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
   val name: String,
   /**
    * The namespace for this type to be used when creating
    * an avro record. This method takes into account @AvroNamespace.
    */
   val namespace: String?
) {
   val fullName : String get() = if (namespace != null) "$namespace$name" else name

   companion object {
      operator fun invoke(name: String, annotations: List<Annotation>, namingStrategy: NamingStrategy = DefaultNamingStrategy): RecordNaming {
         val className = if (name.contains('<')) name
            .replaceFirst(".<init>", "")
            .replaceFirst(".<anonymous>", "") else name
         val annotationExtractor = AnnotationExtractor(annotations)
         val avroName = annotationExtractor.name() ?: className.substringAfterLast('.')
         val namespace = annotationExtractor.namespace()
            ?: className.substringBeforeLast('.', missingDelimiterValue = "").takeIf { it.isNotEmpty() }
         return RecordNaming(
            name = namingStrategy.to(avroName),
            namespace = namespace
         )
      }

      operator fun invoke(descriptor: SerialDescriptor, namingStrategy: NamingStrategy = DefaultNamingStrategy): RecordNaming = RecordNaming(
         if (descriptor.isNullable) descriptor.serialName.removeSuffix("?") else descriptor.serialName,
         descriptor.annotations,
         namingStrategy
      )
   }
}
