package com.github.avrokotlin.avro4k

import com.github.avrokotlin.avro4k.internal.AvroGenerated
import com.squareup.kotlinpoet.AnnotationSpec
import com.squareup.kotlinpoet.ClassName
import com.squareup.kotlinpoet.CodeBlock
import com.squareup.kotlinpoet.asClassName
import kotlinx.serialization.Contextual
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonPrimitive

internal fun buildAvroFixedAnnotation(schema: AvroSchema): AnnotationSpec? {
    if (schema !is AvroSchema.FixedSchema) {
        return null
    }
    return AnnotationSpec.builder(AvroFixed::class.asClassName())
        .addMember(CodeBlock.of("${AvroFixed::size.name} = %L", schema.size))
        .build()
}

internal fun buildAvroPropAnnotations(carrier: WithProps): List<AnnotationSpec> {
    return buildList {
        carrier.props.forEach { (key, value) ->
            add(
                AnnotationSpec.builder(AvroProp::class.asClassName())
                    .addMember(CodeBlock.of("%S, %S", key, value.contentUnquoted))
                    .build()
            )
        }
    }
}

internal fun buildAvroAliasAnnotation(carrier: AvroSchema.NamedSchema): AnnotationSpec? {
    return buildAvroAliasAnnotation(carrier.aliases.map { it.fullName })
}

internal fun buildAvroAliasAnnotation(aliases: Collection<String>): AnnotationSpec? {
    if (aliases.isNotEmpty()) {
        return AnnotationSpec.builder(AvroAlias::class.asClassName())
            .apply {
                aliases.forEach { alias ->
                    addMember(CodeBlock.of("%S", alias))
                }
            }
            .build()
    }
    return null
}

internal fun buildAvroDecimalAnnotation(schema: AvroSchema): AnnotationSpec? {
    if ((schema is AvroSchema.FixedSchema || schema is AvroSchema.BytesSchema) && schema.logicalTypeName == "decimal") {
        val scale = schema.props["scale"]?.jsonPrimitive?.intOrNull
        val precision = schema.props["precision"]?.jsonPrimitive?.intOrNull
        precision ?: error("Missing 'precision' prop for 'decimal' logical type of schema $schema")

        return AnnotationSpec.builder(AvroDecimal::class.asClassName())
            .addMember(CodeBlock.of("${AvroDecimal::scale.name} = %L", scale ?: 0))
            .addMember(CodeBlock.of("${AvroDecimal::precision.name} = %L", precision))
            .build()
    }
    return null
}

internal fun buildAvroGeneratedAnnotation(schema: AvroSchema): AnnotationSpec {
    return AnnotationSpec.builder(AvroGenerated::class.asClassName())
        .addMember("%P", schema.toJsonElement().toString())
        .build()
}

internal fun buildAvroDefaultAnnotation(field: AvroSchema.RecordSchema.Field): AnnotationSpec? {
    if (field.defaultValue == null) {
        return null
    }
    return buildAvroDefaultAnnotation(field.defaultValue.contentUnquoted)
}

private fun buildAvroDefaultAnnotation(unquotedDefaultValue: String): AnnotationSpec {
    return AnnotationSpec.builder(AvroDefault::class.asClassName())
        .addMember("%S", unquotedDefaultValue)
        .build()
}

internal fun buildImplicitAvroDefaultAnnotation(schema: AvroSchema, implicitNulls: Boolean, implicitEmptyCollections: Boolean): AnnotationSpec? {
    if (implicitNulls && schema.isNullable) {
        return buildAvroDefaultAnnotation("null")
    } else if (implicitEmptyCollections) {
        if (schema is AvroSchema.ArraySchema) {
            return buildAvroDefaultAnnotation("[]")
        } else if (schema is AvroSchema.MapSchema) {
            return buildAvroDefaultAnnotation("{}")
        }
    }
    return null
}

internal fun buildImplicitAvroDefaultCodeBlock(schema: AvroSchema, implicitNulls: Boolean, implicitEmptyCollections: Boolean): CodeBlock? {
    if (implicitNulls && schema.isNullable) {
        return CodeBlock.of("null")
    } else if (implicitEmptyCollections) {
        if (schema is AvroSchema.ArraySchema) {
            return getListOfCodeBlock(emptyList())
        } else if (schema is AvroSchema.MapSchema) {
            return getMapOfCodeBlock(emptyMap())
        }
    }
    return null
}

internal fun buildAvroDocAnnotation(carrier: WithDoc): AnnotationSpec? {
    if (carrier.doc != null) {
        return AnnotationSpec.builder(AvroDoc::class.asClassName())
            .addMember(CodeBlock.of("%S", carrier.doc))
            .build()
    }
    return null
}

internal fun buildSerializableAnnotation(kSerializerType: ClassName): AnnotationSpec =
    AnnotationSpec.builder(Serializable::class)
        .addMember("${Serializable::with.name} = %T::class", kSerializerType)
        .build()

internal fun buildContextualAnnotation(): AnnotationSpec =
    AnnotationSpec.builder(Contextual::class.asClassName()).build()

private val JsonElement.contentUnquoted: String get() = if (this is JsonPrimitive && isString) content else toString()