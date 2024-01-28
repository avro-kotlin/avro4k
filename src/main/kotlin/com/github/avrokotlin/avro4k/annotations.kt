@file:OptIn(ExperimentalSerializationApi::class)

package com.github.avrokotlin.avro4k

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerialInfo
import kotlinx.serialization.descriptors.PrimitiveKind
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.intellij.lang.annotations.Language

@SerialInfo
@Target(AnnotationTarget.PROPERTY, AnnotationTarget.CLASS)
annotation class AvroProp(val key: String, val value: String)

@SerialInfo
@Target(AnnotationTarget.PROPERTY, AnnotationTarget.CLASS)
annotation class AvroJsonProp(
    val key: String,
    @Language("JSON") val jsonValue: String,
)

@SerialInfo
@Target(AnnotationTarget.PROPERTY, AnnotationTarget.CLASS)
annotation class AvroNamespace(val value: String)

@SerialInfo
@Target(AnnotationTarget.PROPERTY, AnnotationTarget.CLASS)
annotation class AvroName(val value: String)

@SerialInfo
@Target(AnnotationTarget.PROPERTY, AnnotationTarget.CLASS)
annotation class ScalePrecision(val scale: Int = 2, val precision: Int = 8)

@SerialInfo
@Target(AnnotationTarget.PROPERTY)
annotation class AvroDecimalLogicalType(val schema: LogicalDecimalTypeEnum = LogicalDecimalTypeEnum.BYTES)

enum class LogicalDecimalTypeEnum {
    BYTES,
    STRING,

    /**
     * Fixed must be accompanied with [AvroFixed]
     */
    FIXED,
}

@SerialInfo
@Target(AnnotationTarget.PROPERTY)
annotation class AvroUuidLogicalType

@SerialInfo
@Target(AnnotationTarget.PROPERTY)
annotation class AvroTimeLogicalType(val type: LogicalTimeTypeEnum)

enum class LogicalTimeTypeEnum(val logicalTypeName: String, val kind: PrimitiveKind, val schemaFor: () -> Schema) {
    DATE("date", PrimitiveKind.INT, { LogicalTypes.date().addToSchema(SchemaBuilder.builder().intType()) }),
    TIME_MILLIS(
        "time-millis",
        PrimitiveKind.INT,
        { LogicalTypes.timeMillis().addToSchema(SchemaBuilder.builder().intType()) }
    ),
    TIME_MICROS(
        "time-micros",
        PrimitiveKind.LONG,
        { LogicalTypes.timeMicros().addToSchema(SchemaBuilder.builder().longType()) }
    ),
    TIMESTAMP_MILLIS(
        "timestamp-millis",
        PrimitiveKind.LONG,
        { LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder().longType()) }
    ),
    TIMESTAMP_MICROS(
        "timestamp-micros",
        PrimitiveKind.LONG,
        { LogicalTypes.timestampMicros().addToSchema(SchemaBuilder.builder().longType()) }
    ),
    LOCAL_TIMESTAMP_MILLIS(
        "local-timestamp-millis",
        PrimitiveKind.LONG,
        { LogicalTypes.localTimestampMillis().addToSchema(SchemaBuilder.builder().longType()) }
    ),
    LOCAL_TIMESTAMP_MICROS(
        "local-timestamp-micros",
        PrimitiveKind.LONG,
        { LogicalTypes.localTimestampMicros().addToSchema(SchemaBuilder.builder().longType()) }
    ),
}

@SerialInfo
@Target(AnnotationTarget.CLASS)
annotation class AvroInline

@SerialInfo
@Target(AnnotationTarget.PROPERTY, AnnotationTarget.CLASS)
annotation class AvroDoc(val value: String)

@SerialInfo
@Target(AnnotationTarget.PROPERTY, AnnotationTarget.CLASS)
annotation class AvroAlias(vararg val value: String)

@SerialInfo
@Deprecated(
    message = "Will be removed in the next major release",
    replaceWith = ReplaceWith("@AvroAlias(alias1, alias2)")
)
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
annotation class AvroDefault(
    @Language("JSON") val value: String,
)

@SerialInfo
@Target(AnnotationTarget.CLASS)
annotation class AvroEnumDefault(val value: String)