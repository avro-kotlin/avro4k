package com.github.avrokotlin.avro4k.schema

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.fasterxml.jackson.databind.node.NullNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.node.TextNode
import com.github.avrokotlin.avro4k.AnnotatedLocation
import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroAlias
import com.github.avrokotlin.avro4k.AvroDefault
import com.github.avrokotlin.avro4k.AvroDoc
import com.github.avrokotlin.avro4k.AvroEnumDefault
import com.github.avrokotlin.avro4k.AvroFixed
import com.github.avrokotlin.avro4k.AvroJsonProp
import com.github.avrokotlin.avro4k.AvroLogicalType
import com.github.avrokotlin.avro4k.AvroNamespaceOverride
import com.github.avrokotlin.avro4k.AvroProp
import com.github.avrokotlin.avro4k.AvroSchema
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.SerialKind
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.boolean
import kotlinx.serialization.json.booleanOrNull
import org.apache.avro.JsonProperties
import org.apache.avro.Schema

internal data class VisitorContext(
    val avro: Avro,
    val resolvedSchemas: MutableMap<RecordName, Schema>,
    val json: Json,
    val inlinedAnnotations: ValueAnnotations? = null,
)

internal fun VisitorContext.resetNesting() = copy(inlinedAnnotations = null)

internal interface AvroVisitorContextAware {
    val context: VisitorContext
}

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
    val jsonProps: Sequence<AvroJsonProp>,
    val aliases: Sequence<AvroAlias>,
    val doc: AvroDoc?,
    val default: AvroDefault?,
    val namespaceOverride: AvroNamespaceOverride?,
) {
    constructor(descriptor: SerialDescriptor, elementIndex: Int) : this(
        descriptor.findElementAnnotations<AvroProp>(elementIndex).asSequence(),
        descriptor.findElementAnnotations<AvroJsonProp>(elementIndex).asSequence(),
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
    val jsonProps: Sequence<AvroJsonProp>,
    val aliases: AvroAlias?,
    val doc: AvroDoc?,
    val enumDefault: AvroEnumDefault?,
) {
    constructor(descriptor: SerialDescriptor) : this(
        descriptor.findAnnotations<AvroProp>().asSequence(),
        descriptor.findAnnotations<AvroJsonProp>().asSequence(),
        descriptor.findAnnotation<AvroAlias>(),
        descriptor.findAnnotation<AvroDoc>(),
        descriptor.findAnnotation<AvroEnumDefault>()
    ) {
        if (enumDefault != null) {
            require(descriptor.kind == SerialKind.ENUM) { "@AvroEnumDefault can only be used on enums. Actual: $descriptor" }
        } else {
            require(descriptor.kind == StructureKind.CLASS || descriptor.kind == StructureKind.OBJECT || descriptor.kind == SerialKind.ENUM) {
                "TypeAnnotations are only for classes, objects and enums. Actual: $descriptor"
            }
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

context(AvroVisitorContextAware)
internal val AvroJsonProp.jsonNode: JsonNode
    get() {
        if (jsonValue.isStartingAsJson()) {
            return context.json.parseToJsonElement(jsonValue).toJacksonNode()
        }
        return TextNode.valueOf(jsonValue)
    }

context(AvroVisitorContextAware)
internal val AvroDefault.jsonValue: Any
    get() {
        if (value.isStartingAsJson()) {
            return context.json.parseToJsonElement(value).toAvroObject()
        }
        return value
    }

/**
 * Returns true if the given content is starting with `"`, {`, `[`, a digit or equals to `null`.
 * It doesn't check if the content is valid json.
 * It skips the whitespaces at the beginning of the content.
 */
internal fun String.isStartingAsJson(): Boolean {
    val i = this.indexOfFirst { !it.isWhitespace() }
    if (i == -1) {
        return false
    }
    val c = this[i]
    return c == '{' || c == '"' || c.isDigit() || c == '[' || this == "null" || this == "true" || this == "false"
}

private fun JsonElement.toAvroObject(): Any =
    when (this) {
        is JsonNull -> JsonProperties.NULL_VALUE
        is JsonObject -> this.entries.associate { it.key to it.value.toAvroObject() }
        is JsonArray -> this.map { it.toAvroObject() }
        is JsonPrimitive ->
            when {
                this.isString -> this.content
                this.booleanOrNull != null -> this.boolean
                else -> {
                    this.content.toBigDecimal().stripTrailingZeros().let {
                        if (it.scale() <= 0) it.toBigInteger() else it
                    }
                }
            }
    }

private fun JsonElement.toJacksonNode(): JsonNode =
    when (this) {
        is JsonNull -> NullNode.instance
        is JsonObject -> ObjectNode(JsonNodeFactory.instance, this.entries.associate { it.key to it.value.toJacksonNode() })
        is JsonArray -> ArrayNode(JsonNodeFactory.instance, this.map { it.toJacksonNode() })
        is JsonPrimitive ->
            when {
                this.isString -> JsonNodeFactory.instance.textNode(this.content)
                this.booleanOrNull != null -> JsonNodeFactory.instance.booleanNode(this.boolean)
                else ->
                    this.content.toBigDecimal().let {
                        if (it.scale() <= 0) JsonNodeFactory.instance.numberNode(it.toBigInteger()) else JsonNodeFactory.instance.numberNode(it)
                    }
            }
    }

/**
 * Get the record/enum name using the configured record naming strategy.
 */
context(AvroVisitorContextAware)
internal fun SerialDescriptor.getAvroName() = context.avro.configuration.recordNamingStrategy.resolve(this, serialName)

/**
 * Get the field name using the configured field naming strategy.
 */
context(AvroVisitorContextAware)
internal fun SerialDescriptor.getElementAvroName(elementIndex: Int) = context.avro.configuration.fieldNamingStrategy.resolve(this, elementIndex, getElementName(elementIndex))