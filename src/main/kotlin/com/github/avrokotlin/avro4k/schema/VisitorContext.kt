package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.AnnotatedLocation
import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroAlias
import com.github.avrokotlin.avro4k.AvroDefault
import com.github.avrokotlin.avro4k.AvroDoc
import com.github.avrokotlin.avro4k.AvroFixed
import com.github.avrokotlin.avro4k.AvroLogicalType
import com.github.avrokotlin.avro4k.AvroNamespaceOverride
import com.github.avrokotlin.avro4k.AvroProp
import com.github.avrokotlin.avro4k.AvroSchema
import com.github.avrokotlin.avro4k.internal.findAnnotation
import com.github.avrokotlin.avro4k.internal.findAnnotations
import com.github.avrokotlin.avro4k.internal.findElementAnnotation
import com.github.avrokotlin.avro4k.internal.findElementAnnotations
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.SerialKind
import kotlinx.serialization.descriptors.StructureKind
import org.apache.avro.Schema

internal data class VisitorContext(
    val avro: Avro,
    val resolvedSchemas: MutableMap<String, Schema>,
    val inlinedAnnotations: ValueAnnotations? = null,
)

internal fun VisitorContext.resetNesting() = copy(inlinedAnnotations = null)

/**
 * Contains all the annotations for a field of a class (kind == CLASS && isInline == true).
 */
internal data class InlineClassFieldAnnotations(
    val namespaceOverride: AvroNamespaceOverride?,
) {
    constructor(descriptor: SerialDescriptor, elementIndex: Int) : this(
        descriptor.findElementAnnotation<AvroNamespaceOverride>(elementIndex)
    ) {
        require(descriptor.isInline) {
            "${InlineClassFieldAnnotations::class.qualifiedName} is only for inline classes, but trying at element index $elementIndex with non-inline class descriptor $descriptor"
        }
    }
}

/**
 * Contains all the annotations for a field of a class (kind == CLASS && isInline == false).
 */
internal data class FieldAnnotations(
    val props: Sequence<AvroProp>,
    val aliases: Sequence<AvroAlias>,
    val doc: AvroDoc?,
    val default: AvroDefault?,
    val namespaceOverride: AvroNamespaceOverride?,
) {
    constructor(descriptor: SerialDescriptor, elementIndex: Int) : this(
        descriptor.findElementAnnotations<AvroProp>(elementIndex).asSequence(),
        descriptor.findElementAnnotations<AvroAlias>(elementIndex).asSequence(),
        descriptor.findElementAnnotation<AvroDoc>(elementIndex),
        descriptor.findElementAnnotation<AvroDefault>(elementIndex),
        descriptor.findElementAnnotation<AvroNamespaceOverride>(elementIndex)
    ) {
        require(descriptor.kind == StructureKind.CLASS) {
            "${FieldAnnotations::class.qualifiedName} is only for classes, but trying at element index $elementIndex with non class descriptor $descriptor"
        }
    }
}

/**
 * Contains all the annotations for a field of a class, inline or not (kind == CLASS).
 * Helpful when nesting multiple inline classes to get the first annotation.
 */
internal data class ValueAnnotations(
    val stack: List<AnnotatedLocation>,
    val fixed: AnnotatedElementOrType<AvroFixed>?,
    val customSchema: AnnotatedElementOrType<AvroSchema>?,
    val logicalType: AnnotatedElementOrType<AvroLogicalType>?,
) {
    constructor(descriptor: SerialDescriptor, elementIndex: Int) : this(
        listOf(SimpleAnnotatedLocation(descriptor, elementIndex)),
        AnnotatedElementOrType<AvroFixed>(descriptor, elementIndex),
        AnnotatedElementOrType<AvroSchema>(descriptor, elementIndex),
        AnnotatedElementOrType<AvroLogicalType>(descriptor, elementIndex)
    )

    constructor(descriptor: SerialDescriptor) : this(
        listOf(SimpleAnnotatedLocation(descriptor)),
        AnnotatedElementOrType<AvroFixed>(descriptor),
        AnnotatedElementOrType<AvroSchema>(descriptor),
        AnnotatedElementOrType<AvroLogicalType>(descriptor)
    )
}

internal data class AnnotatedElementOrType<T : Annotation>(
    override val descriptor: SerialDescriptor,
    override val elementIndex: Int?,
    val annotation: T,
) : AnnotatedLocation {
    companion object {
        inline operator fun <reified T : Annotation> invoke(
            descriptor: SerialDescriptor,
            elementIndex: Int,
        ) = descriptor.findElementAnnotation<T>(elementIndex)?.let { AnnotatedElementOrType(descriptor, elementIndex, it) }

        inline operator fun <reified T : Annotation> invoke(descriptor: SerialDescriptor) = descriptor.findAnnotation<T>()?.let { AnnotatedElementOrType(descriptor, null, it) }
    }
}

internal data class SimpleAnnotatedLocation(
    override val descriptor: SerialDescriptor,
    override val elementIndex: Int? = null,
) : AnnotatedLocation

/**
 * Contains all the annotations for a class, object or enum (kind == CLASS || kind == OBJECT || kind == ENUM).
 */
internal data class TypeAnnotations(
    val props: Sequence<AvroProp>,
    val aliases: AvroAlias?,
    val doc: AvroDoc?,
) {
    constructor(descriptor: SerialDescriptor) : this(
        descriptor.findAnnotations<AvroProp>().asSequence(),
        descriptor.findAnnotation<AvroAlias>(),
        descriptor.findAnnotation<AvroDoc>()
    ) {
        require(descriptor.kind == StructureKind.CLASS || descriptor.kind == StructureKind.OBJECT || descriptor.kind == SerialKind.ENUM) {
            "TypeAnnotations are only for classes, objects and enums. Actual: $descriptor"
        }
    }
}

/**
 * Keep the top-est annotation. If the current element details annotation is null, it will be replaced by the new annotation.
 * If the current element details annotation is not null, it will be kept.
 */
internal fun ValueAnnotations?.appendAnnotations(other: ValueAnnotations) =
    ValueAnnotations(
        fixed = this?.fixed ?: other.fixed,
        logicalType = this?.logicalType ?: other.logicalType,
        customSchema = this?.customSchema ?: other.customSchema,
        stack = (this?.stack ?: emptyList()) + other.stack
    )