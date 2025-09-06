package com.github.avrokotlin.avro4k

import com.squareup.kotlinpoet.ClassName
import com.squareup.kotlinpoet.CodeBlock
import com.squareup.kotlinpoet.FunSpec
import com.squareup.kotlinpoet.KModifier
import com.squareup.kotlinpoet.ParameterSpec
import com.squareup.kotlinpoet.PropertySpec
import com.squareup.kotlinpoet.TypeSpec
import com.squareup.kotlinpoet.asClassName

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