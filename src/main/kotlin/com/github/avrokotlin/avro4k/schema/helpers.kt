package com.github.avrokotlin.avro4k.schema

import kotlinx.serialization.descriptors.SerialDescriptor
import org.apache.avro.Schema

inline fun <reified T : Annotation> SerialDescriptor.findAnnotation() = annotations.firstNotNullOfOrNull { it as? T }

inline fun <reified T : Annotation> SerialDescriptor.findAnnotations() = annotations.filterIsInstance<T>()

inline fun <reified T : Annotation> SerialDescriptor.findElementAnnotation(elementIndex: Int) = getElementAnnotations(elementIndex).firstNotNullOfOrNull { it as? T }

inline fun <reified T : Annotation> SerialDescriptor.findElementAnnotations(elementIndex: Int) = getElementAnnotations(elementIndex).filterIsInstance<T>()

internal val SerialDescriptor.nonNullSerialName: String get() = serialName.removeSuffix('?')

private fun String.removeSuffix(suffix: Char): String {
    if (lastOrNull() == suffix) {
        return substring(0, length - 1)
    }
    return this
}

internal val Schema.nonNull: Schema
    get() =
        when {
            type == Schema.Type.UNION && isNullable -> this.types.filter { it.type != Schema.Type.NULL }.let { if (it.size > 1) Schema.createUnion(it) else it[0] }
            else -> this
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