package com.github.avrokotlin.avro4k.internal

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.TextNode
import com.github.avrokotlin.avro4k.AvroAlias
import com.github.avrokotlin.avro4k.AvroProp
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.descriptors.PolymorphicKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.SerialKind
import kotlinx.serialization.descriptors.capturedKClass
import kotlinx.serialization.descriptors.elementDescriptors
import kotlinx.serialization.descriptors.getContextualDescriptor
import kotlinx.serialization.descriptors.getPolymorphicDescriptors
import kotlinx.serialization.descriptors.nonNullOriginal
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.serializerOrNull
import org.apache.avro.LogicalType
import org.apache.avro.Schema

internal inline fun <reified T : Annotation> SerialDescriptor.findAnnotation() = annotations.firstNotNullOfOrNull { it as? T }

internal inline fun <reified T : Annotation> SerialDescriptor.findAnnotations() = annotations.filterIsInstance<T>()

@PublishedApi
internal inline fun <reified T : Annotation> SerialDescriptor.findElementAnnotation(elementIndex: Int): T? = getElementAnnotations(elementIndex).firstNotNullOfOrNull { it as? T }

internal inline fun <reified T : Annotation> SerialDescriptor.findElementAnnotations(elementIndex: Int) = getElementAnnotations(elementIndex).filterIsInstance<T>()

internal val SerialDescriptor.nonNullSerialName: String get() = nonNullOriginal.serialName
internal val SerialDescriptor.namespace: String? get() = serialName.substringBeforeLast('.', "").takeIf { it.isNotEmpty() }

internal val Schema.nullable: Schema
    get() {
        if (isNullable) return this
        return if (isUnion) {
            Schema.createUnion(listOf(Schema.create(Schema.Type.NULL)) + this.types)
        } else {
            Schema.createUnion(Schema.create(Schema.Type.NULL), this)
        }
    }

internal fun Schema.asSchemaList(): List<Schema> {
    if (!isUnion) return listOf(this)
    return types
}

internal fun Schema.isNamedSchema(): Boolean {
    return this.type == Schema.Type.RECORD || this.type == Schema.Type.ENUM || this.type == Schema.Type.FIXED
}

internal fun Schema.isFullNameOrAliasMatch(descriptor: SerialDescriptor): Boolean {
    return isFullNameMatch(descriptor.nonNullSerialName) || descriptor.aliases.any { isFullNameMatch(it) }
}

internal fun Schema.isFullNameMatch(fullNameToMatch: String): Boolean {
    return fullName == fullNameToMatch ||
        (type == Schema.Type.RECORD || type == Schema.Type.ENUM || type == Schema.Type.FIXED) &&
        aliases.any { it == fullNameToMatch }
}

internal val SerialDescriptor.aliases: Set<String>
    get() =
        findAnnotation<AvroAlias>()?.value?.toSet() ?: emptySet()

private val SCHEMA_PLACEHOLDER = Schema.create(Schema.Type.NULL)

internal fun Schema.copy(
    name: String = this.name,
    doc: String? = this.doc,
    namespace: String? = if (this.type == Schema.Type.RECORD || this.type == Schema.Type.ENUM || this.type == Schema.Type.FIXED) this.namespace else null,
    aliases: Set<String> = if (this.type == Schema.Type.RECORD || this.type == Schema.Type.ENUM || this.type == Schema.Type.FIXED) this.aliases else emptySet(),
    isError: Boolean = if (this.type == Schema.Type.RECORD) this.isError else false,
    types: List<Schema> = if (this.isUnion) this.types.toList() else emptyList(),
    enumSymbols: List<String> = if (this.type == Schema.Type.ENUM) this.enumSymbols.toList() else emptyList(),
    fields: List<Schema.Field>? = if (this.type == Schema.Type.RECORD && this.hasFields()) this.fields.map { it.copy() } else null,
    enumDefault: String? = if (this.type == Schema.Type.ENUM) this.enumDefault else null,
    fixedSize: Int = if (this.type == Schema.Type.FIXED) this.fixedSize else -1,
    valueType: Schema = if (this.type == Schema.Type.MAP) this.valueType else SCHEMA_PLACEHOLDER,
    elementType: Schema = if (this.type == Schema.Type.ARRAY) this.elementType else SCHEMA_PLACEHOLDER,
    objectProps: Map<String, Any> = this.objectProps,
    additionalProps: Map<String, Any> = emptyMap(),
    logicalType: LogicalType? = this.logicalType,
): Schema {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    return when (type) {
        Schema.Type.RECORD -> if (hasFields()) Schema.createRecord(name, doc, namespace, isError, fields) else Schema.createRecord(name, doc, namespace, isError)
        Schema.Type.ENUM -> Schema.createEnum(name, doc, namespace, enumSymbols, enumDefault)
        Schema.Type.FIXED -> Schema.createFixed(name, doc, namespace, fixedSize)

        Schema.Type.UNION -> Schema.createUnion(types)
        Schema.Type.MAP -> Schema.createMap(valueType)
        Schema.Type.ARRAY -> Schema.createArray(elementType)
        Schema.Type.BYTES,
        Schema.Type.STRING,
        Schema.Type.INT,
        Schema.Type.LONG,
        Schema.Type.FLOAT,
        Schema.Type.DOUBLE,
        Schema.Type.BOOLEAN,
        Schema.Type.NULL,
        -> Schema.create(type)
    }
        .also { newSchema ->
            objectProps.forEach { (key, value) -> newSchema.addProp(key, value) }
            additionalProps.forEach { (key, value) -> newSchema.addProp(key, value) }
            logicalType?.addToSchema(newSchema)
            aliases.forEach { newSchema.addAlias(it) }
        }
}

internal fun Schema.Field.copy(
    name: String = this.name(),
    schema: Schema = this.schema(),
    doc: String? = this.doc(),
    defaultVal: Any? = this.defaultVal(),
    order: Schema.Field.Order = this.order(),
    aliases: Set<String> = this.aliases(),
    objectProps: Map<String, Any> = this.objectProps,
    additionalProps: Map<String, Any> = emptyMap(),
): Schema.Field {
    return Schema.Field(name, schema, doc, defaultVal, order)
        .also { newSchema ->
            objectProps.forEach { (key, value) -> newSchema.addProp(key, value) }
            additionalProps.forEach { (key, value) -> newSchema.addProp(key, value) }
            aliases.forEach { newSchema.addAlias(it) }
        }
}

@ExperimentalSerializationApi
internal fun SerialDescriptor.possibleSerializationSubclasses(serializersModule: SerializersModule): Sequence<SerialDescriptor> {
    return when (this.kind) {
        PolymorphicKind.SEALED ->
            elementDescriptors.asSequence()
                .filter { it.kind == SerialKind.CONTEXTUAL }
                .flatMap { it.elementDescriptors }
                .flatMap { it.possibleSerializationSubclasses(serializersModule) }

        PolymorphicKind.OPEN ->
            serializersModule.getPolymorphicDescriptors(this@possibleSerializationSubclasses).asSequence()
                .flatMap { it.possibleSerializationSubclasses(serializersModule) }

        SerialKind.CONTEXTUAL -> sequenceOf(getNonNullContextualDescriptor(serializersModule))

        else -> sequenceOf(this)
    }
}

@OptIn(InternalSerializationApi::class)
internal fun SerialDescriptor.getNonNullContextualDescriptor(serializersModule: SerializersModule) =
    requireNotNull(serializersModule.getContextualDescriptor(this) ?: this.capturedKClass?.serializerOrNull()?.descriptor) {
        "No descriptor found in serialization context for $this"
    }

/**
 * Returns true if the given content is starting with `"`, {`, `[`, a digit or equals to `null`.
 * It doesn't check if the content is valid json.
 * It skips the whitespaces at the beginning of the content.
 */
internal fun String.isStartingAsJson(): Boolean {
    val i = this.indexOfFirst { !it.isWhitespace() }
    if (i == -1) {
        return false
    }
    val c = this[i]
    return c == '{' || c == '"' || c == '[' || c.isDigit() || this == "null" || this == "true" || this == "false"
}

private val objectMapper by lazy { ObjectMapper() }

internal val AvroProp.jsonNode: JsonNode
    get() {
        if (value.isStartingAsJson()) {
            return objectMapper.readTree(value)
        }
        return TextNode.valueOf(value)
    }

internal fun SerialDescriptor.getElementIndexNullable(name: String): Int? {
    return getElementIndex(name).takeIf { it >= 0 }
}