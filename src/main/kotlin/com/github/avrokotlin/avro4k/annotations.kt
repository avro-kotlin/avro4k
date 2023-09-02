@file:OptIn(ExperimentalSerializationApi::class)
package com.github.avrokotlin.avro4k

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerialInfo
import org.intellij.lang.annotations.Language

@SerialInfo
@Target(AnnotationTarget.PROPERTY, AnnotationTarget.CLASS)
annotation class AvroProp(val key: String, val value: String)

@SerialInfo
@Target(AnnotationTarget.PROPERTY, AnnotationTarget.CLASS)
annotation class AvroJsonProp(val key: String, @Language("JSON") val jsonValue: String)

/**
 * When applied on a type, changes the namespace of the type.
 * When applied on a field, changes the namespace of the field type if it's a fixed schema type. Works with [AvroFixed]
 */
@SerialInfo
@Target(AnnotationTarget.PROPERTY, AnnotationTarget.CLASS)
annotation class AvroNamespace(val value: String)

/**
 * When applied on a type, changes the name of the type.
 * When applied on a field, changes the name of the field (Take note that [com.github.avrokotlin.avro4k.schema.NamingStrategy] from configuration is used).
 */
@SerialInfo
@Target(AnnotationTarget.PROPERTY, AnnotationTarget.CLASS)
annotation class AvroName(val value: String)

@SerialInfo
@Target(AnnotationTarget.PROPERTY, AnnotationTarget.CLASS)
annotation class ScalePrecision(val scale: Int, val precision: Int)

@SerialInfo
@Target(AnnotationTarget.PROPERTY, AnnotationTarget.CLASS)
annotation class AvroDoc(val value: String)

@SerialInfo
@Target(AnnotationTarget.PROPERTY, AnnotationTarget.CLASS)
annotation class AvroAlias(vararg val value: String)

@SerialInfo
@Deprecated(message = "Will be removed in the next major release", replaceWith = ReplaceWith("@AvroAlias(alias1, alias2)"))
@Target(AnnotationTarget.PROPERTY, AnnotationTarget.CLASS)
annotation class AvroAliases(val value: Array<String>)

/**
 * [AvroFixed] overrides the schema type for a field or a value class
 * so that the schema is set to org.apache.avro.Schema.Type.FIXED
 * rather than whatever the default would be.
 *
 * This annotation can be used in the following ways:
 *
 * - On a field, eg data class `Foo(@AvroField(10) val name: String)`
 * which results in the field `name` having schema type FIXED with
 * a size of 10.
 *
 * - On a value type, eg `@AvroField(7) data class Foo(val name: String)`
 * which results in all usages of this type having schema
 * FIXED with a size of 7 rather than the default.
 */
@SerialInfo
@Target(AnnotationTarget.PROPERTY, AnnotationTarget.CLASS)
annotation class AvroFixed(val size: Int)

@SerialInfo
@Target(AnnotationTarget.PROPERTY, AnnotationTarget.CLASS)
annotation class AvroDefault(@Language("JSON") val value: String)

@SerialInfo
@Target(AnnotationTarget.CLASS)
annotation class AvroEnumDefault(val value: String)
