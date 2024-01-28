package com.github.avrokotlin.avro4k

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor

@ExperimentalSerializationApi
class AnnotationExtractor(private val annotations: List<Annotation>) {
    companion object {
        fun entity(descriptor: SerialDescriptor) =
            AnnotationExtractor(
                descriptor.annotations
            )

        operator fun invoke(
            descriptor: SerialDescriptor,
            index: Int,
        ): AnnotationExtractor = AnnotationExtractor(descriptor.getElementAnnotations(index))
    }

    fun fixed(): Int? = annotations.filterIsInstance<AvroFixed>().firstOrNull()?.size

    fun doc(): String? = annotations.filterIsInstance<AvroDoc>().firstOrNull()?.value

    fun aliases(): List<String> =
        (
            annotations.firstNotNullOfOrNull {
                it as? AvroAlias
            }?.value ?: emptyArray()
        ).asList()

    fun props(): List<Pair<String, String>> = annotations.filterIsInstance<AvroProp>().map { it.key to it.value }

    fun jsonProps(): List<Pair<String, String>> = annotations.filterIsInstance<AvroJsonProp>().map { it.key to it.jsonValue }

    fun default(): String? = annotations.filterIsInstance<AvroDefault>().firstOrNull()?.value

    fun enumDefault(): String? = annotations.filterIsInstance<AvroEnumDefault>().firstOrNull()?.value
}