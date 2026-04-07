@file:OptIn(ExperimentalSerializationApi::class)

package com.github.avrokotlin.avro4k

import com.github.avrokotlin.avro4k.serializer.BigDecimalSerializer
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerialInfo

/**
 * To be used with [BigDecimalSerializer] to specify the scale, precision, type and rounding mode of the decimal value.
 *
 * Can be used with [AvroFixed] to serialize value as a fixed type.
 *
 * Only works with [java.math.BigDecimal] property type.
 */
@SerialInfo
@ExperimentalAvro4kApi
@Target(AnnotationTarget.PROPERTY)
public annotation class AvroDecimal(
    val scale: Int,
    val precision: Int,
)