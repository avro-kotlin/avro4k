package com.github.avrokotlin.avro4k

import com.fasterxml.jackson.databind.node.BinaryNode
import com.github.avrokotlin.avro4k.internal.AvroGenerated
import com.squareup.kotlinpoet.AnnotationSpec
import com.squareup.kotlinpoet.CodeBlock
import com.squareup.kotlinpoet.asClassName
import org.apache.avro.util.internal.JacksonUtils

internal fun buildAvroFixedAnnotation(schema: TypeSafeSchema): AnnotationSpec? {
    if (schema !is TypeSafeSchema.NamedSchema.FixedSchema) {
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
                    .addMember(CodeBlock.of("%S, %S", key, value.toString()))
                    .build()
            )
        }
    }
}

internal fun buildAvroAliasAnnotation(carrier: WithAliases): AnnotationSpec? {
    if (carrier.aliases.isNotEmpty()) {
        return AnnotationSpec.builder(AvroAlias::class.asClassName())
            .apply {
                carrier.aliases.forEach { alias ->
                    addMember(CodeBlock.of("%S", alias))
                }
            }
            .build()
    }
    return null
}

internal fun buildAvroDecimalAnnotation(schema: TypeSafeSchema): AnnotationSpec? {
    if ((schema is TypeSafeSchema.NamedSchema.FixedSchema || schema is TypeSafeSchema.PrimitiveSchema.BytesSchema) && schema.logicalTypeName == "decimal") {
        val scale: Int? by schema.props
        val precision: Int? by schema.props
        precision ?: error("Missing 'precision' prop for 'decimal' logical type of schema $schema")

        return AnnotationSpec.builder(AvroDecimal::class.asClassName())
            .addMember(CodeBlock.of("${AvroDecimal::scale.name} = %L", scale ?: 0))
            .addMember(CodeBlock.of("${AvroDecimal::precision.name} = %L", precision))
            .build()
    }
    return null
}

internal fun buildAvroGeneratedAnnotation(schemaStr: String): AnnotationSpec {
    return AnnotationSpec.builder(AvroGenerated::class.asClassName())
        .addMember("%P", schemaStr)
        .build()
}

internal fun buildAvroDefaultAnnotation(field: TypeSafeSchema.NamedSchema.RecordSchema.Field): AnnotationSpec? {
    if (!field.hasDefaultValue()) {
        return null
    }
    return buildAvroDefaultAnnotation(field.unquotedDefaultValue())
}

private fun buildAvroDefaultAnnotation(unquotedDefaultValue: String): AnnotationSpec {
    return AnnotationSpec.builder(AvroDefault::class.asClassName())
        .addMember("%S", unquotedDefaultValue)
        .build()
}

private fun TypeSafeSchema.NamedSchema.RecordSchema.Field.unquotedDefaultValue(): String =
    JacksonUtils.toJsonNode(defaultValue)
        ?.let {
            if (it.isTextual) {
                it.asText()
            } else if (it is BinaryNode) {
                it.binaryValue().toString(Charsets.ISO_8859_1)
            } else {
                it.toString()
            }
        } ?: "null"

internal fun buildImplicitAvroDefaultAnnotation(schema: TypeSafeSchema, implicitNulls: Boolean, implicitEmptyCollections: Boolean): AnnotationSpec? {
    if (implicitNulls && schema.isNullable) {
        return buildAvroDefaultAnnotation("null")
    } else if (implicitEmptyCollections) {
        if (schema is TypeSafeSchema.CollectionSchema.ArraySchema) {
            return buildAvroDefaultAnnotation("[]")
        } else if (schema is TypeSafeSchema.CollectionSchema.MapSchema) {
            return buildAvroDefaultAnnotation("{}")
        }
    }
    return null
}

internal fun buildImplicitAvroDefaultCodeBlock(schema: TypeSafeSchema, implicitNulls: Boolean, implicitEmptyCollections: Boolean): CodeBlock? {
    if (implicitNulls && schema.isNullable) {
        return CodeBlock.of("null")
    } else if (implicitEmptyCollections) {
        if (schema is TypeSafeSchema.CollectionSchema.ArraySchema) {
            return getListOfCodeBlock(emptyList())
        } else if (schema is TypeSafeSchema.CollectionSchema.MapSchema) {
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