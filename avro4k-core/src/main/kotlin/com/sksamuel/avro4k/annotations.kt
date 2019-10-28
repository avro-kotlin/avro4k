package com.sksamuel.avro4k

import kotlinx.serialization.SerialInfo

@SerialInfo
@Target(AnnotationTarget.PROPERTY, AnnotationTarget.CLASS)
annotation class AvroProp(val key: String, val value: String)

@SerialInfo
@Target(AnnotationTarget.PROPERTY, AnnotationTarget.CLASS)
annotation class AvroNamespace(val value: String)

@SerialInfo
@Target(AnnotationTarget.PROPERTY, AnnotationTarget.CLASS)
annotation class AvroName(val value: String)

@SerialInfo
@Target(AnnotationTarget.PROPERTY, AnnotationTarget.CLASS)
annotation class ScalePrecision(val scale: Int, val precision: Int)

@SerialInfo
@Target(AnnotationTarget.CLASS)
annotation class AvroInline

@SerialInfo
@Target(AnnotationTarget.PROPERTY, AnnotationTarget.CLASS)
annotation class AvroDoc(val value: String)

@SerialInfo
@Target(AnnotationTarget.PROPERTY, AnnotationTarget.CLASS)
annotation class AvroAlias(val value: String)

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