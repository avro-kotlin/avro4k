@file:Suppress("FunctionName")

package com.github.avrokotlin.avro4k.internal

import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.SerialKind
import kotlinx.serialization.encoding.Decoder
import kotlin.reflect.KClass

internal class AvroSchemaGenerationException(message: String) : SerializationException(message)

context(Decoder)
internal fun DecodedNullError() = SerializationException("Unexpected null value, Decoder.decodeTaggedNotNullMark should be called first")

context(Decoder)
internal fun DecodedNullError(
    descriptor: SerialDescriptor,
    elementIndex: Int,
) = SerializationException(
    "Unexpected null value for field '${descriptor.getElementName(elementIndex)}' for type '${descriptor.serialName}', Decoder.decodeTaggedNotNullMark should be called first"
)

internal fun Decoder.IllegalIndexedAccessError() = UnsupportedOperationException("${this::class.qualifiedName} does not support indexed access")

context(Decoder)
internal inline fun <reified ExpectedType> BadDecodedValueError(
    value: Any?,
    firstExpectedType: KClass<*>,
    vararg expectedTypes: KClass<*>,
): SerializationException {
    val allExpectedTypes = listOf(firstExpectedType) + expectedTypes
    return if (value == null) {
        SerializationException(
            "Decoded null value for ${ExpectedType::class.qualifiedName} kind, expected one of [${allExpectedTypes.joinToString { it.qualifiedName!! }}]"
        )
    } else {
        SerializationException(
            "Decoded value '$value' of type ${value::class.qualifiedName} for " +
                "${ExpectedType::class.qualifiedName} kind, expected one of [${allExpectedTypes.joinToString { it.qualifiedName!! }}]"
        )
    }
}

context(Decoder)
internal fun BadDecodedValueError(
    value: Any?,
    expectedKind: SerialKind,
    firstExpectedType: KClass<*>,
    vararg expectedTypes: KClass<*>,
): SerializationException {
    val allExpectedTypes = listOf(firstExpectedType) + expectedTypes
    return if (value == null) {
        SerializationException(
            "Decoded null value for $expectedKind kind, expected one of [${allExpectedTypes.joinToString { it.qualifiedName!! }}]"
        )
    } else {
        SerializationException(
            "Decoded value '$value' of type ${value::class.qualifiedName} for $expectedKind kind, expected one of [${allExpectedTypes.joinToString { it.qualifiedName!! }}]"
        )
    }
}