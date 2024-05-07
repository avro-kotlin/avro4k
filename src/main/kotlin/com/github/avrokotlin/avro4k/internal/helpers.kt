package com.github.avrokotlin.avro4k.internal

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.TextNode
import com.github.avrokotlin.avro4k.AvroProp
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.PolymorphicKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.SerialKind
import kotlinx.serialization.descriptors.capturedKClass
import kotlinx.serialization.descriptors.elementDescriptors
import kotlinx.serialization.descriptors.getContextualDescriptor
import kotlinx.serialization.descriptors.getPolymorphicDescriptors
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.serializerOrNull
import org.apache.avro.Schema

internal inline fun <reified T : Annotation> SerialDescriptor.findAnnotation() = annotations.firstNotNullOfOrNull { it as? T }

internal inline fun <reified T : Annotation> SerialDescriptor.findAnnotations() = annotations.filterIsInstance<T>()

internal inline fun <reified T : Annotation> SerialDescriptor.findElementAnnotation(elementIndex: Int) = getElementAnnotations(elementIndex).firstNotNullOfOrNull { it as? T }

internal inline fun <reified T : Annotation> SerialDescriptor.findElementAnnotations(elementIndex: Int) = getElementAnnotations(elementIndex).filterIsInstance<T>()

internal val SerialDescriptor.nonNullSerialName: String get() = serialName.removeSuffix('?')

private fun String.removeSuffix(suffix: Char): String {
    if (lastOrNull() == suffix) {
        return substring(0, length - 1)
    }
    return this
}

/**
 * Overrides the namespace of a [Schema] with the given namespace.
 */
internal fun Schema.overrideNamespace(namespaceOverride: String): Schema {
    return when (type) {
        Schema.Type.RECORD -> {
            val fields =
                fields.map { field ->
                    Schema.Field(
                        field.name(),
                        field.schema().overrideNamespace(namespaceOverride),
                        field.doc(),
                        field.defaultVal(),
                        field.order()
                    )
                }
            val copy = Schema.createRecord(name, doc, namespaceOverride, isError, fields)
            aliases.forEach { copy.addAlias(it) }
            copy
        }

        Schema.Type.UNION -> Schema.createUnion(types.map { it.overrideNamespace(namespaceOverride) })
        Schema.Type.ENUM ->
            Schema.createEnum(name, doc, namespaceOverride, enumSymbols, enumDefault)
                .also { aliases.forEach { alias -> it.addAlias(alias) } }

        Schema.Type.FIXED ->
            Schema.createFixed(name, doc, namespaceOverride, fixedSize)
                .also { aliases.forEach { alias -> it.addAlias(alias) } }

        Schema.Type.MAP -> Schema.createMap(valueType.overrideNamespace(namespaceOverride))
        Schema.Type.ARRAY -> Schema.createArray(elementType.overrideNamespace(namespaceOverride))
        else -> this
    }
        .also { objectProps.forEach { prop -> it.addProp(prop.key, prop.value) } }
}

context(Encoder)
internal fun Schema.ensureTypeOf(type: Schema.Type) {
    if (this.type != type) {
        throw SerializationException("Schema $this must be of type $type to be used with ${this@ensureTypeOf::class}")
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