package com.github.avrokotlin.avro4k

import com.squareup.kotlinpoet.Annotatable
import com.squareup.kotlinpoet.AnnotationSpec
import com.squareup.kotlinpoet.ClassName
import com.squareup.kotlinpoet.CodeBlock
import com.squareup.kotlinpoet.Documentable
import com.squareup.kotlinpoet.FunSpec
import com.squareup.kotlinpoet.KModifier
import com.squareup.kotlinpoet.MemberName
import com.squareup.kotlinpoet.ParameterSpec
import com.squareup.kotlinpoet.PropertySpec
import com.squareup.kotlinpoet.TypeName
import com.squareup.kotlinpoet.TypeSpec
import com.squareup.kotlinpoet.asClassName
import com.squareup.kotlinpoet.joinToCode

/**
 * Adds a property to the class and also adds it as a parameter to the primary constructor.
 * This is helpful for data or value classes where properties are typically defined in the primary constructor.
 */
internal fun TypeSpec.Builder.addPrimaryProperty(property: PropertySpec, defaultValue: CodeBlock? = null): TypeSpec.Builder {
    val typeSpec =
        addProperty(property.toBuilder().initializer(property.name).build())
            .build()
    val ctor =
        typeSpec
            .primaryConstructor
            ?.toBuilder()
            ?: FunSpec.constructorBuilder()
    return typeSpec.toBuilder()
        .primaryConstructor(
            ctor.addParameter(
                ParameterSpec.builder(property.name, property.type)
                    .defaultValue(defaultValue)
                    .build()
            ).build()
        )
}

/**
 * Generates equals and hashCode functions, while comparing ByteArray properties with contentEquals and contentHashCode
 * to avoid reference equality issues.
 */
internal fun TypeSpec.Builder.addEqualsHashCode(className: ClassName): TypeSpec.Builder {
    if (propertySpecs.none { it.type == ByteArray::class.asClassName() }) {
        // we only generate equals/hashcode if a field has ByteArray type
        return this
    }
    addFunction(
        FunSpec.builder("equals")
            .addModifiers(KModifier.OVERRIDE)
            .addParameter("other", Any::class.asClassName().copy(nullable = true))
            .returns(Boolean::class)
            .addStatement("if (this === other) return true")
            .addStatement("if (javaClass != other?.javaClass) return false")
            .addStatement("other as %T", className)
            .apply {
                propertySpecs.forEach { property ->
                    if (property.type == ByteArray::class.asClassName()) {
                        addStatement("if (!%N.contentEquals(other.%N)) return false", property.name, property.name)
                    } else {
                        addStatement("if (%N != other.%N) return false", property.name, property.name)
                    }
                }
            }
            .addStatement("return true")
            .build()
    )
    addFunction(
        FunSpec.builder("hashCode")
            .addModifiers(KModifier.OVERRIDE)
            .returns(Int::class)
            .apply {
                propertySpecs.first().let { property ->
                    val hashCodeMethodName = if (property.type == ByteArray::class.asClassName()) "contentHashCode" else "hashCode"
                    addStatement("var result = %N.%N()", property.name, hashCodeMethodName)
                }
                propertySpecs.drop(1).forEach { property ->
                    val hashCodeMethodName = if (property.type == ByteArray::class.asClassName()) "contentHashCode" else "hashCode"
                    addStatement("result = 31 * result + %N.%N()", property.name, hashCodeMethodName)
                }
                addStatement("return result")
            }
            .build()
    )
    return this
}

internal fun TypeSpec.withAnnotation(annotation: AnnotationSpec): TypeSpec {
    return toBuilder()
        .addAnnotation(annotation)
        .build()
}

internal fun <T : Annotatable.Builder<T>> T.addAnnotationIfNotNull(annotation: AnnotationSpec?): T {
    return annotation?.let { addAnnotation(it) } ?: this
}

internal fun <T : Documentable.Builder<T>> T.addKDocIfNotNull(doc: String?): T {
    return doc?.let { addKdoc(it) } ?: this
}

internal fun getMapOfCodeBlock(map: Map<String, CodeBlock>): CodeBlock =
    if (map.isNotEmpty()) {
        CodeBlock.of(
            "%M(%L)",
            MemberName("kotlin.collections", "mapOf"),
            map.map { (key, value) -> CodeBlock.of("%S to %L", key, value) }.joinToCode()
        )
    } else {
        CodeBlock.of("%M()", MemberName("kotlin.collections", "emptyMap"))
    }

internal fun getListOfCodeBlock(list: List<CodeBlock>): CodeBlock =
    if (list.isNotEmpty()) {
        CodeBlock.of("%M(%L)", MemberName("kotlin.collections", "listOf"), list.joinToCode())
    } else {
        CodeBlock.of("%M()", MemberName("kotlin.collections", "emptyList"))
    }

internal fun getKotlinClassReplacement(className: String): ClassName? =
    when (className) {
        String::class.java.name -> String::class.asClassName()
        Boolean::class.javaObjectType.name, Boolean::class.javaPrimitiveType!!.name -> Boolean::class.asClassName()
        Char::class.javaObjectType.name, Char::class.javaPrimitiveType!!.name -> Char::class.asClassName()
        Byte::class.javaObjectType.name, Byte::class.javaPrimitiveType!!.name -> Byte::class.asClassName()
        Short::class.javaObjectType.name, Short::class.javaPrimitiveType!!.name -> Short::class.asClassName()
        Int::class.javaObjectType.name, Int::class.javaPrimitiveType!!.name -> Int::class.asClassName()
        Long::class.javaObjectType.name, Long::class.javaPrimitiveType!!.name -> Long::class.asClassName()
        Float::class.javaObjectType.name, Float::class.javaPrimitiveType!!.name -> Float::class.asClassName()
        Double::class.javaObjectType.name, Double::class.javaPrimitiveType!!.name -> Double::class.asClassName()
        else -> null
    }

internal fun ClassName.Companion.fromFullName(fullName: String) =
    ClassName.bestGuess(fullName)

internal fun TypeName.nullableIf(toBeNullable: Boolean): TypeName {
    if (toBeNullable == isNullable) return this
    return copy(nullable = toBeNullable)
}