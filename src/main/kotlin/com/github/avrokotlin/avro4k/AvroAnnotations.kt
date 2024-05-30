@file:OptIn(ExperimentalSerializationApi::class)

package com.github.avrokotlin.avro4k

import com.github.avrokotlin.avro4k.internal.asAvroLogicalType
import com.github.avrokotlin.avro4k.internal.nonNullSerialName
import com.github.avrokotlin.avro4k.serializer.BigDecimalSerializer
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerialInfo
import kotlinx.serialization.descriptors.SerialDescriptor
import org.apache.avro.LogicalType
import org.intellij.lang.annotations.Language

/**
 * When annotated on a property, deeply overrides the namespace for all the nested named types (records, enums and fixed).
 *
 * Works with standard classes and inline classes.
 */
@SerialInfo
@ExperimentalSerializationApi
@Target(AnnotationTarget.PROPERTY)
public annotation class AvroNamespaceOverride(
    val value: String,
)

/**
 * Adds a property to the Avro schema or field. Its value could be any valid JSON or just a string.
 *
 * When annotated on a value class or its underlying field, the props are applied to the underlying type.
 */
@SerialInfo
@Repeatable
@Target(AnnotationTarget.PROPERTY, AnnotationTarget.CLASS)
public annotation class AvroProp(
    val key: String,
    @Language("JSON") val value: String,
)

/**
 * To be used with [BigDecimalSerializer] to specify the scale, precision, type and rounding mode of the decimal value.
 *
 * Can be used with [AvroFixed] to serialize value as a fixed type.
 */
@SerialInfo
@ExperimentalSerializationApi
@Target(AnnotationTarget.PROPERTY)
public annotation class AvroDecimal(
    val scale: Int = 2,
    val precision: Int = 8,
)

/**
 * Adds documentation to:
 * - a record's field
 * - a record
 * - an enum
 *
 * Ignored in inline classes.
 */
@SerialInfo
@Target(AnnotationTarget.PROPERTY, AnnotationTarget.CLASS)
public annotation class AvroDoc(val value: String)

/**
 * Adds aliases to a field of a record. It helps to allow having different names for the same field for better compatibility when changing a schema.
 *
 * Ignored in inline classes.
 *
 * @param value The aliases for the annotated property. Note that the given aliases won't be changed by the configured [AvroConfiguration.fieldNamingStrategy].
 */
@SerialInfo
@Target(AnnotationTarget.PROPERTY, AnnotationTarget.CLASS)
public annotation class AvroAlias(vararg val value: String)

/**
 * Indicates that the annotated property should be encoded as an Avro fixed type.
 * @param size The number of bytes of the fixed type. Note that smaller values will be padded with 0s during encoding, but not unpadded when decoding.
 */
@SerialInfo
@Target(AnnotationTarget.PROPERTY)
public annotation class AvroFixed(val size: Int)

/**
 * Sets the default avro value for a record's field.
 *
 * Ignored in inline classes.
 */
@SerialInfo
@Target(AnnotationTarget.PROPERTY)
public annotation class AvroDefault(
    @Language("JSON") val value: String,
)

/**
 * Sets the enum default value when decoded an unknown enum value.
 *
 * It must be annotated on an enum value. Otherwise, it will be ignored.
 */
@SerialInfo
@ExperimentalSerializationApi
@Target(AnnotationTarget.PROPERTY)
public annotation class AvroEnumDefault

/**
 * Adds a logical type to the given serializer, where the logical type name is the descriptor's name.
 *
 * To use it:
 * ```kotlin
 * object YourCustomLogicalTypeSerializer : KSerializer<YourType> {
 *   override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor("YourType", PrimitiveKind.STRING)
 *                                                   .asAvroLogicalType()
 * }
 * ```
 *
 * For more complex needs, please file an issue [here](https://github.com/avro-kotlin/avro4k/issues).
 */
@ExperimentalSerializationApi
public fun SerialDescriptor.asAvroLogicalType(): SerialDescriptor {
    return asAvroLogicalType { LogicalType(nonNullSerialName) }
}