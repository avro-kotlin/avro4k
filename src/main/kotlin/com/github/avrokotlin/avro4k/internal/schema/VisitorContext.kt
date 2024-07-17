package com.github.avrokotlin.avro4k.internal.schema

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroAlias
import com.github.avrokotlin.avro4k.AvroConfiguration
import com.github.avrokotlin.avro4k.AvroDefault
import com.github.avrokotlin.avro4k.AvroDoc
import com.github.avrokotlin.avro4k.AvroProp
import com.github.avrokotlin.avro4k.internal.findAnnotation
import com.github.avrokotlin.avro4k.internal.findAnnotations
import com.github.avrokotlin.avro4k.internal.findElementAnnotation
import com.github.avrokotlin.avro4k.internal.findElementAnnotations
import com.github.avrokotlin.avro4k.serializer.ElementLocation
import com.github.avrokotlin.avro4k.serializer.SchemaSupplierContext
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.SerialKind
import kotlinx.serialization.descriptors.StructureKind
import org.apache.avro.Schema

internal data class VisitorContext(
    val avro: Avro,
    val resolvedSchemas: MutableMap<String, Schema>,
    override val inlinedElements: List<ElementLocation> = emptyList(),
) : SchemaSupplierContext {
    override val configuration: AvroConfiguration
        get() = avro.configuration
}

/**
 * Contains all the annotations for a field of a class (kind == CLASS && isInline == true).
 */
internal data class InlineClassFieldAnnotations(
    val props: Sequence<AvroProp>,
) {
    constructor(inlineClassDescriptor: SerialDescriptor) : this(
        sequence {
            yieldAll(inlineClassDescriptor.findAnnotations<AvroProp>())
            yieldAll(inlineClassDescriptor.findElementAnnotations<AvroProp>(0))
        }
    ) {
        require(inlineClassDescriptor.isInline) {
            "${InlineClassFieldAnnotations::class.qualifiedName} is only for inline classes, but trying to use it with non-inline class descriptor $inlineClassDescriptor"
        }
    }
}

/**
 * Contains all the annotations for a field of a class (kind == CLASS && isInline == false).
 */
internal data class FieldAnnotations(
    val props: Sequence<AvroProp>,
    val aliases: AvroAlias?,
    val doc: AvroDoc?,
    val default: AvroDefault?,
) {
    constructor(descriptor: SerialDescriptor, elementIndex: Int) : this(
        descriptor.findElementAnnotations<AvroProp>(elementIndex).asSequence(),
        descriptor.findElementAnnotation<AvroAlias>(elementIndex),
        descriptor.findElementAnnotation<AvroDoc>(elementIndex),
        descriptor.findElementAnnotation<AvroDefault>(elementIndex)
    ) {
        require(descriptor.kind == StructureKind.CLASS) {
            "${FieldAnnotations::class.qualifiedName} is only for classes, but trying at element index $elementIndex with non class descriptor $descriptor"
        }
    }
}

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