package com.github.avrokotlin.avro4k

import com.github.avrokotlin.avro4k.internal.aliases
import com.github.avrokotlin.avro4k.internal.isNamedSchema
import com.github.avrokotlin.avro4k.internal.nonNullSerialName
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Encoder
import org.apache.avro.Schema

/**
 * Interface to encode Avro values.
 * Here are the main methods to encode values. Each encode method is adapting the type to the raw type, that means unions are resolved if needed, and also all primitives are converted automatically (a wanted `int` could be encoded to a `long`).
 * - [encodeNull]
 * - [encodeBoolean]
 * - [encodeByte]
 * - [encodeShort]
 * - [encodeInt]
 * - [encodeLong]
 * - [encodeFloat]
 * - [encodeDouble]
 * - [encodeString]
 * - [encodeChar]
 * - [encodeEnum]
 * - [encodeBytes]
 * - [encodeFixed]
 */
public interface AvroEncoder : Encoder {
    /**
     * Provides the schema used to encode the current value.
     */
    @ExperimentalAvro4kApi
    public val currentWriterSchema: Schema

    /**
     * Encodes a [Schema.Type.BYTES] value from a [ByteArray].
     */
    @ExperimentalAvro4kApi
    public fun encodeBytes(value: ByteArray)

    /**
     * Encodes a [Schema.Type.FIXED] value from a [ByteArray]. Its size must match the size of the fixed schema in [currentWriterSchema].
     * When many fixed schemas are in a union, the first one that matches the size is selected. To avoid this auto-selection, use [encodeUnionIndex] with the index of the expected fixed schema.
     */
    @ExperimentalAvro4kApi
    public fun encodeFixed(value: ByteArray)

    /**
     * Selects the index of the union type to encode. Also sets [currentWriterSchema] to the selected type.
     */
    @ExperimentalAvro4kApi
    public fun encodeUnionIndex(index: Int)
}

internal fun AvroEncoder.namedSchemaNotFoundInUnionError(
    expectedName: String,
    possibleAliases: Set<String>,
    vararg fallbackTypes: Schema.Type,
): Throwable {
    val aliasesStr = if (possibleAliases.isNotEmpty()) " (with aliases ${possibleAliases.joinToString()})" else ""
    val fallbacksStr = if (fallbackTypes.isNotEmpty()) " Also no compatible type found (one of ${fallbackTypes.joinToString()})." else ""
    return SerializationException("Named schema $expectedName$aliasesStr not found in union.$fallbacksStr Actual schema: $currentWriterSchema")
}

internal fun AvroEncoder.unsupportedWriterTypeError(
    mainType: Schema.Type,
    vararg fallbackTypes: Schema.Type,
): Throwable {
    val fallbacksStr = if (fallbackTypes.isNotEmpty()) ", and also not matching to any compatible type (one of ${fallbackTypes.joinToString()})." else ""
    return SerializationException(
        "Unsupported schema '${currentWriterSchema.fullName}' for encoded type of ${mainType.getName()}$fallbacksStr. Actual schema: $currentWriterSchema"
    )
}

internal fun AvroEncoder.ensureFixedSize(byteArray: ByteArray): ByteArray {
    if (currentWriterSchema.fixedSize != byteArray.size) {
        throw SerializationException("Fixed size mismatch for actual size of ${byteArray.size}. Actual schema: $currentWriterSchema")
    }
    return byteArray
}

internal fun AvroEncoder.fullNameOrAliasMismatchError(
    fullName: String,
    aliases: Set<String>,
): Throwable {
    val aliasesStr = if (aliases.isNotEmpty()) " (with aliases ${aliases.joinToString()})" else ""
    return SerializationException("The descriptor $fullName$aliasesStr doesn't match the schema $currentWriterSchema")
}

internal fun AvroEncoder.logicalTypeMismatchError(
    logicalType: String,
    type: Schema.Type,
): Throwable {
    return SerializationException("Expected schema type of ${type.getName()} with logical type $logicalType but had schema $currentWriterSchema")
}

internal fun AvroEncoder.trySelectTypeNameFromUnion(expectedType: Schema.Type): Boolean {
    val index =
        currentWriterSchema.getIndexTyped(expectedType)
            ?: return false
    encodeUnionIndex(index)
    return true
}

internal fun AvroEncoder.trySelectFixedSchemaForSize(fixedSize: Int): Boolean {
    currentWriterSchema.types.forEachIndexed { index, schema ->
        if (schema.type == Schema.Type.FIXED && schema.fixedSize == fixedSize) {
            encodeUnionIndex(index)
            return true
        }
    }
    return false
}

internal fun AvroEncoder.trySelectEnumSchemaForSymbol(symbol: String): Boolean {
    currentWriterSchema.types.forEachIndexed { index, schema ->
        if (schema.type == Schema.Type.ENUM && schema.hasEnumSymbol(symbol)) {
            encodeUnionIndex(index)
            return true
        }
    }
    return false
}

internal fun AvroEncoder.trySelectNamedSchema(descriptor: SerialDescriptor): Boolean {
    return trySelectNamedSchema(descriptor.nonNullSerialName, descriptor::aliases)
}

internal fun AvroEncoder.trySelectNamedSchema(
    name: String,
    aliases: () -> Set<String> = ::emptySet,
): Boolean {
    val index =
        currentWriterSchema.getIndexNamedOrAliased(name)
            ?: aliases().firstNotNullOfOrNull { currentWriterSchema.getIndexNamedOrAliased(it) }
    if (index != null) {
        encodeUnionIndex(index)
        return true
    }
    return false
}

internal fun AvroEncoder.trySelectLogicalTypeFromUnion(
    logicalTypeName: String,
    vararg oneOf: Schema.Type,
): Boolean {
    val index =
        currentWriterSchema.getIndexLogicallyTyped(logicalTypeName, *oneOf)
            ?: return false
    encodeUnionIndex(index)
    return true
}

internal fun Schema.getIndexLogicallyTyped(
    logicalTypeName: String,
    vararg oneOf: Schema.Type,
): Int? {
    return oneOf.firstNotNullOfOrNull { expectedType ->
        when (expectedType) {
            Schema.Type.FIXED, Schema.Type.RECORD, Schema.Type.ENUM -> types.indexOfFirst { it.type == expectedType && it.logicalType?.name == logicalTypeName }.takeIf { it >= 0 }
            else -> getIndexNamed(expectedType.getName())?.takeIf { types[it].logicalType?.name == logicalTypeName }
        }
    }
}

internal fun Schema.getIndexNamedOrAliased(expectedName: String): Int? {
    return getIndexNamed(expectedName)
        ?: types.indexOfFirst { it.isNamedSchema() && it.aliases.contains(expectedName) }.takeIf { it >= 0 }
}

internal fun Schema.getIndexTyped(expectedType: Schema.Type): Int? {
    @Suppress("UsePropertyAccessSyntax") // We want to use the getter method as it doesn't return the same as the kotlin property "name"
    return getIndexNamed(expectedType.getName())
}