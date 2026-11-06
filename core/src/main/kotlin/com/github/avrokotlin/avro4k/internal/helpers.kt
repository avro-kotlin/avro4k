package com.github.avrokotlin.avro4k.internal

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.TextNode
import com.github.avrokotlin.avro4k.AvroAlias
import com.github.avrokotlin.avro4k.AvroDefault
import com.github.avrokotlin.avro4k.AvroProp
import com.github.avrokotlin.avro4k.InternalAvro4kApi
import com.github.avrokotlin.avro4k.internal.schema.CHAR_LOGICAL_TYPE_NAME
import kotlinx.io.Buffer
import kotlinx.io.UnsafeIoApi
import kotlinx.io.unsafe.UnsafeBufferOperations
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.PolymorphicKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.SerialKind
import kotlinx.serialization.descriptors.capturedKClass
import kotlinx.serialization.descriptors.elementDescriptors
import kotlinx.serialization.descriptors.getContextualDescriptor
import kotlinx.serialization.descriptors.getPolymorphicDescriptors
import kotlinx.serialization.descriptors.nonNullOriginal
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.serializerOrNull
import org.apache.avro.LogicalType
import org.apache.avro.Schema

internal inline fun <reified T : Annotation> SerialDescriptor.findAnnotation() = annotations.firstNotNullOfOrNull { it as? T }

internal inline fun <reified T : Annotation> SerialDescriptor.findAnnotations() = annotations.filterIsInstance<T>()

@PublishedApi
internal inline fun <reified T : Annotation> SerialDescriptor.findElementAnnotation(elementIndex: Int): T? = getElementAnnotations(elementIndex).firstNotNullOfOrNull { it as? T }

internal inline fun <reified T : Annotation> SerialDescriptor.findElementAnnotations(elementIndex: Int) = getElementAnnotations(elementIndex).filterIsInstance<T>()

@OptIn(ExperimentalSerializationApi::class)
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

@InternalAvro4kApi
public fun Schema.isNamedSchema(): Boolean {
    return when (this.type) {
        Schema.Type.RECORD, Schema.Type.ENUM, Schema.Type.FIXED -> true
        else -> false
    }
}

internal fun Schema.nonFailingAliases(): Set<String> {
    return if (isNamedSchema()) {
        aliases
    } else {
        emptySet()
    }
}

internal fun Schema.isFullNameOrAliasMatch(descriptor: SerialDescriptor): Boolean {
    return isFullNameOrAliasMatch(descriptor.nonNullSerialName, descriptor::aliases)
}

internal fun Schema.isFullNameOrAliasMatch(
    fullName: String,
    aliases: () -> Set<String>,
): Boolean {
    return isFullNameMatch(fullName) || aliases().any { isFullNameMatch(it) }
}

internal fun Schema.isFullNameMatch(fullNameToMatch: String): Boolean {
    return fullName == fullNameToMatch || (isNamedSchema() && aliases.any { it == fullNameToMatch })
}

@InternalAvro4kApi
public val SerialDescriptor.aliases: Set<String>
    get() =
        findAnnotation<AvroAlias>()?.value?.toSet() ?: emptySet()

internal fun SerialDescriptor.getElementAliases(elementIndex: Int): Set<String> =
    findElementAnnotation<AvroAlias>(elementIndex)?.value?.toSet() ?: emptySet()

private val SCHEMA_PLACEHOLDER = Schema.create(Schema.Type.NULL)

@InternalAvro4kApi
public fun Schema.copy(
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
    logicalTypeName: String? = this.logicalType?.name,
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
            if (logicalType != null && logicalTypeName != null) {
                require(logicalType.name == logicalTypeName) { "Logical type name mismatch: expected '$logicalTypeName', but got '${logicalType.name}'" }
            }
            (logicalType ?: logicalTypeName?.let(::LogicalType))?.addToSchema(newSchema)
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

@OptIn(ExperimentalSerializationApi::class)
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

@OptIn(ExperimentalSerializationApi::class)
internal fun SerialDescriptor.getNonNullContextualDescriptor(serializersModule: SerializersModule) =
    requireNotNull(serializersModule.getContextualDescriptor(this) ?: this.capturedKClass?.java?.let(serializersModule::serializerOrNull)?.descriptor) {
        "No descriptor found in serialization context for $this"
    }

private val strictObjectReader by lazy { ObjectMapper().reader().with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS) }

internal val AvroProp.jsonNode: JsonNode
    get() =
        try {
            strictObjectReader.readTree(value)?.takeUnless { it.isMissingNode }
        } catch (_: JsonProcessingException) {
            null
        } ?: TextNode.valueOf(value)

/**
 * Reads the default of a field with the given schema, so that the schema and the decoder read it the same way:
 * - a string field keeps the value as written, unless it is a json string or `null`
 * - a value that is json is read as json
 * - a numeric field reads a number that is not json, such as `+1`, as that number
 * - anything else is the value as written
 */
internal fun AvroDefault.toJsonElement(schema: Schema): JsonElement {
    val json = value.toJsonElementOrNull()
    return when {
        schema.isString() && json !is JsonNull && !(json is JsonPrimitive && json.isString) -> JsonPrimitive(value)
        json != null -> json
        schema.isNumeric() -> value.trim().toBigDecimalOrNull()?.let { JsonPrimitive(it) } ?: JsonPrimitive(value)
        else -> JsonPrimitive(value)
    }
}

/**
 * Parses the content as json, or returns null when it is not json, such as `hello`, `12abc` or `1 day`.
 */
private fun String.toJsonElementOrNull(): JsonElement? =
    try {
        Json.parseToJsonElement(this).takeIf { it.isJson() }
    } catch (_: SerializationException) {
        null
    }

// kotlinx reads a word that is not quoted, like `hello` or `12abc`, as a literal, which json does not allow
private fun JsonElement.isJson(): Boolean =
    when (this) {
        is JsonObject -> values.all { it.isJson() }
        is JsonArray -> all { it.isJson() }
        is JsonNull -> true
        is JsonPrimitive -> isString || booleanOrNull != null || content.toBigDecimalOrNull() != null
    }

internal fun Schema.isString(): Boolean = nonNullTypesAll { it.type == Schema.Type.STRING }

internal fun Schema.isNumeric(): Boolean = nonNullTypesAll { it.type in NUMERIC_TYPES && it.logicalType?.name != CHAR_LOGICAL_TYPE_NAME }

internal fun Schema.isFloatingPoint(): Boolean = nonNullTypesAll { it.type == Schema.Type.FLOAT || it.type == Schema.Type.DOUBLE }

private inline fun Schema.nonNullTypesAll(predicate: (Schema) -> Boolean): Boolean {
    val types = asSchemaList().filter { it.type != Schema.Type.NULL }
    return types.isNotEmpty() && types.all(predicate)
}

private val NUMERIC_TYPES = setOf(Schema.Type.INT, Schema.Type.LONG, Schema.Type.FLOAT, Schema.Type.DOUBLE)

@OptIn(UnsafeIoApi::class)
internal fun Buffer(bytes: ByteArray): Buffer =
    Buffer().apply { UnsafeBufferOperations.moveToTail(this, bytes) }