@file:OptIn(ExperimentalSerializationApi::class)

package com.github.avrokotlin.avro4k

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerialInfo
import kotlinx.serialization.descriptors.PrimitiveKind
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.intellij.lang.annotations.Language

/**
 * When annotated on a property, overrides the namespace for the nested record.
 */
@SerialInfo
@Target(AnnotationTarget.PROPERTY)
annotation class AvroNamespaceOverride(val value: String)

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
@Target(AnnotationTarget.PROPERTY)
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

enum class LogicalTimeTypeEnum(val kind: PrimitiveKind, val schemaFor: () -> Schema) {
    DATE(PrimitiveKind.INT, { LogicalTypes.date().addToSchema(SchemaBuilder.builder().intType()) }),
    TIME_MILLIS(
        PrimitiveKind.INT,
        { LogicalTypes.timeMillis().addToSchema(SchemaBuilder.builder().intType()) }
    ),
    TIME_MICROS(
        PrimitiveKind.LONG,
        { LogicalTypes.timeMicros().addToSchema(SchemaBuilder.builder().longType()) }
    ),
    TIMESTAMP_MILLIS(
        PrimitiveKind.LONG,
        { LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder().longType()) }
    ),
    TIMESTAMP_MICROS(
        PrimitiveKind.LONG,
        { LogicalTypes.timestampMicros().addToSchema(SchemaBuilder.builder().longType()) }
    ),
    LOCAL_TIMESTAMP_MILLIS(
        PrimitiveKind.LONG,
        { LogicalTypes.localTimestampMillis().addToSchema(SchemaBuilder.builder().longType()) }
    ),
    LOCAL_TIMESTAMP_MICROS(
        PrimitiveKind.LONG,
        { LogicalTypes.localTimestampMicros().addToSchema(SchemaBuilder.builder().longType()) }
    ),
}

@SerialInfo
@Target(AnnotationTarget.PROPERTY, AnnotationTarget.CLASS)
annotation class AvroDoc(val value: String)

/**
 * Adds aliases to a field of a record. It helps to allow having different names for the same field for better compatibility when changing a schema.
 * @param value The aliases for the annotated property. Note that the given aliases won't be changed by the configured [AvroConfiguration.fieldNamingStrategy].
 */
@SerialInfo
@Target(AnnotationTarget.PROPERTY, AnnotationTarget.CLASS)
annotation class AvroAlias(vararg val value: String)

/**
 * Indicates that the annotated property should be encoded as an Avro fixed type.
 * @param size The number of bytes of the fixed type. Note that smaller values will be padded with 0s during encoding, but not unpadded when decoding.
 */
@SerialInfo
@Target(AnnotationTarget.PROPERTY)
annotation class AvroFixed(val size: Int)

@SerialInfo
@Target(AnnotationTarget.PROPERTY)
annotation class AvroDefault(
    @Language("JSON") val value: String,
)

@SerialInfo
@Target(AnnotationTarget.CLASS)
annotation class AvroEnumDefault(val value: String)