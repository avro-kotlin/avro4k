package com.github.avrokotlin.avro4k.schema

import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.StructureKind
import org.apache.avro.Schema

inline fun <reified T : Annotation> SerialDescriptor.findAnnotation() = annotations.asSequence().filterIsInstance<T>().firstOrNull()

inline fun <reified T : Annotation> SerialDescriptor.findAnnotations() = annotations.filterIsInstance<T>()

inline fun <reified T : Annotation> SerialDescriptor.findElementAnnotation(elementIndex: Int) = getElementAnnotations(elementIndex).asSequence().filterIsInstance<T>().firstOrNull()

inline fun <reified T : Annotation> SerialDescriptor.findElementAnnotations(elementIndex: Int) = getElementAnnotations(elementIndex).filterIsInstance<T>()

internal fun Schema.extractNonNull(): Schema =
    when (this.type) {
        Schema.Type.UNION -> this.types.filter { it.type != Schema.Type.NULL }.let { if (it.size > 1) Schema.createUnion(it) else it[0] }
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
            this.objectProps.forEach { copy.addProp(it.key, it.value) }
            copy
        }
        Schema.Type.UNION -> Schema.createUnion(types.map { it.overrideNamespace(namespaceOverride) })
        Schema.Type.ENUM -> Schema.createEnum(name, doc, namespaceOverride, enumSymbols, enumDefault)
        Schema.Type.FIXED -> Schema.createFixed(name, doc, namespaceOverride, fixedSize)
        Schema.Type.MAP -> Schema.createMap(valueType.overrideNamespace(namespaceOverride))
        Schema.Type.ARRAY -> Schema.createArray(elementType.overrideNamespace(namespaceOverride))
        else -> this
    }
}

internal fun SerialDescriptor.isByteArray(): Boolean = kind == StructureKind.LIST && getElementDescriptor(0).let { !it.isNullable && it.kind == PrimitiveKind.BYTE }

internal fun PrimitiveKind.toAvroType() =
    when (this) {
        PrimitiveKind.BOOLEAN -> Schema.Type.BOOLEAN
        PrimitiveKind.CHAR -> Schema.Type.INT
        PrimitiveKind.BYTE -> Schema.Type.INT
        PrimitiveKind.SHORT -> Schema.Type.INT
        PrimitiveKind.INT -> Schema.Type.INT
        PrimitiveKind.LONG -> Schema.Type.LONG
        PrimitiveKind.FLOAT -> Schema.Type.FLOAT
        PrimitiveKind.DOUBLE -> Schema.Type.DOUBLE
        PrimitiveKind.STRING -> Schema.Type.STRING
    }