package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.RecordNaming
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.SerialKind
import kotlinx.serialization.descriptors.getContextualDescriptor
import kotlinx.serialization.modules.SerializersModule
import org.apache.avro.Schema

/** creates a union schema type, with nested unions extracted, and duplicate nulls stripped
 * union schemas can't contain other union schemas as a direct
 * child, so whenever we create a union, we need to check if our
 * children are unions and flatten
 */
fun createSafeUnion(nullFirst: Boolean, vararg schemas: Schema): Schema {
    val flattened = schemas.flatMap { schema -> runCatching { schema.types }.getOrElse { listOf(schema) } }
    val (nulls, rest) = flattened.partition { it.type == Schema.Type.NULL }
    return Schema.createUnion(if (nullFirst) nulls + rest else rest + nulls)
}

fun Schema.extractNonNull(): Schema {
    return when (this.type) {
        Schema.Type.UNION -> this.types.filter { !it.isNullable }.let { if (it.size > 1) Schema.createUnion(it) else it[0] }
        else -> this
    }
}

/**
 * Overrides the namespace of a [Schema] with the given namespace.
 */
fun Schema.overrideNamespace(namespace: String): Schema {
    return when (type) {
        Schema.Type.RECORD -> {
            val fields = fields.map { field ->
                Schema.Field(
                    field.name(),
                    field.schema().overrideNamespace(namespace),
                    field.doc(),
                    field.defaultVal(),
                    field.order()
                )
            }
            val copy = Schema.createRecord(name, doc, namespace, isError, fields)
            aliases.forEach { copy.addAlias(it) }
            this.objectProps.forEach { copy.addProp(it.key, it.value) }
            copy
        }

        Schema.Type.UNION -> Schema.createUnion(types.map { it.overrideNamespace(namespace) })
        Schema.Type.ENUM -> Schema.createEnum(name, doc, namespace, enumSymbols, enumDefault)
        Schema.Type.FIXED -> Schema.createFixed(name, doc, namespace, fixedSize)
        Schema.Type.MAP -> Schema.createMap(valueType.overrideNamespace(namespace))
        Schema.Type.ARRAY -> Schema.createArray(elementType.overrideNamespace(namespace))
        else -> this
    }
}

fun Schema.ensureOfType(vararg expectedTypes: Schema.Type) = check(expectedTypes.contains(type)) {
    throw SerializationException("Expected a schema one of type ${expectedTypes.joinToString()} but actual type is $type. Schema: $this")
}

@ExperimentalSerializationApi
fun Schema.getTypeNamed(typeName: RecordNaming): Schema? = getIndexNamed(typeName.fullName)?.let { types[it] }


@ExperimentalSerializationApi
internal fun SerialDescriptor.carrierDescriptor(module: SerializersModule): SerialDescriptor = when {
    kind == SerialKind.CONTEXTUAL -> module.getContextualDescriptor(this)?.carrierDescriptor(module) ?: this
    isInline -> getElementDescriptor(0).carrierDescriptor(module)
    else -> this
}