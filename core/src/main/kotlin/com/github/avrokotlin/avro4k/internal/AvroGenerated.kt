@file:OptIn(ExperimentalSerializationApi::class)

package com.github.avrokotlin.avro4k.internal

import com.github.avrokotlin.avro4k.InternalAvro4kApi
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerialInfo

/**
 * Internal annotation to store the original schema used to generate the annotated class.
 * This is used to ensure that the writer schema used during serialization is exactly the same as the one used during code generation.
 * This annotation is not intended for public use and may change or be removed in future versions.
 *
 * [com.github.avrokotlin.avro4k.Avro.schema] will blindly trust this schema, so if you modify it manually, you may break serialization or deserialization.
 * Do not use this annotation to provide a custom schema for a class. Usee a custom serializer implementing [com.github.avrokotlin.avro4k.serializer.AvroSerializer] instead.
 */
@SerialInfo
@InternalAvro4kApi
@Target(AnnotationTarget.CLASS)
public annotation class AvroGenerated(
    val originalSchema: String,
)