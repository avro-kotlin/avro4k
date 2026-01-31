@file:OptIn(ExperimentalSerializationApi::class)

package com.github.avrokotlin.avro4k

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonObjectBuilder
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.addAll
import kotlinx.serialization.json.addJsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray

internal fun AvroSchema.Companion.fromJsonElement(element: JsonElement, knownNamedTypes: MutableMap<String, AvroSchema> = mutableMapOf()): AvroSchema {
    return fromJsonElement(element, null, knownNamedTypes)
}

private fun fromJsonElement(element: JsonElement, currentSpace: String?, knownNamedTypes: MutableMap<String, AvroSchema>): AvroSchema {
    return when (element) {
        is JsonPrimitive -> {
            // Handle shorthand primitive type or referenced named type
            when (val type = element.contentOrNull) {
                "null" -> AvroSchema.NullSchema()
                "boolean" -> AvroSchema.BooleanSchema()
                "int" -> AvroSchema.IntSchema()
                "long" -> AvroSchema.LongSchema()
                "float" -> AvroSchema.FloatSchema()
                "double" -> AvroSchema.DoubleSchema()
                "bytes" -> AvroSchema.BytesSchema()
                "string" -> AvroSchema.StringSchema()
                else ->
                    knownNamedTypes[type]
                        ?: throw IllegalArgumentException("Unknown named type: $type")
            }
        }

        is JsonArray -> {
            // Handle union
            AvroSchema.UnionSchema(element.map { fromJsonElement(it, currentSpace, knownNamedTypes) })
        }

        is JsonObject -> {
            val props = element.toMutableMap()
            when (val type = props.removeMandatory("type").stringPrimitive) {
                "null" -> AvroSchema.NullSchema(props = props)
                "boolean" -> AvroSchema.BooleanSchema(props = props)
                "int" -> AvroSchema.IntSchema(props = props)
                "long" -> AvroSchema.LongSchema(props = props)
                "float" -> AvroSchema.FloatSchema(props = props)
                "double" -> AvroSchema.DoubleSchema(props = props)
                "bytes" -> AvroSchema.BytesSchema(props = props)
                "string" -> AvroSchema.StringSchema(props = props)

                "map" ->
                    AvroSchema.MapSchema(
                        valueSchema = fromJsonElement(props.removeMandatory("values"), currentSpace, knownNamedTypes),
                        props = props
                    )

                "array" ->
                    AvroSchema.ArraySchema(
                        elementSchema = fromJsonElement(props.removeMandatory("items"), currentSpace, knownNamedTypes),
                        props = props
                    )

                "enum" -> {
                    val name = props.removeName().withSpaceIfMissing(currentSpace)
                    AvroSchema.EnumSchema(
                        name = name,
                        symbols = props.removeMandatory("symbols").jsonArray.map { it.stringPrimitive }.toSet(),
                        defaultSymbol = props.remove("default")?.stringPrimitive,
                        doc = props.remove("doc")?.stringPrimitive,
                        aliases = props.removeAliases(name.space) ?: emptySet(),
                        props = props
                    )
                }

                "fixed" -> {
                    val name = props.removeName().withSpaceIfMissing(currentSpace)
                    AvroSchema.FixedSchema(
                        name = name,
                        size = props.removeMandatory("size").jsonPrimitive.int.toUInt(),
                        doc = props.remove("doc")?.stringPrimitive,
                        aliases = props.removeAliases(name.space) ?: emptySet(),
                        props = props
                    )
                }

                "record" -> {
                    val name = props.removeName().withSpaceIfMissing(currentSpace)
                    AvroSchema.RecordSchema(
                        name = name,
                        fields =
                            props.removeMandatory("fields").jsonArray.map { field ->
                                val fieldProps = field.jsonObject.toMutableMap()
                                AvroSchema.RecordSchema.Field(
                                    name = fieldProps.removeMandatory("name").stringPrimitive,
                                    schema = fromJsonElement(fieldProps.removeMandatory("type"), name.space, knownNamedTypes),
                                    doc = fieldProps.remove("doc")?.stringPrimitive,
                                    aliases = fieldProps.remove("aliases")?.jsonArray?.map { it.stringPrimitive }?.toSet() ?: emptySet(),
                                    // TODO validate record's default, requiring all the fields to be present, and fixed default to be of expected size
                                    defaultValue = fieldProps.remove("default"),
                                    props = fieldProps
                                )
                            },
                        doc = props.remove("doc")?.stringPrimitive,
                        aliases = props.removeAliases(name.space) ?: emptySet(),
                        props = props
                    )
                }

                else -> throw IllegalArgumentException("Unknown schema type: $type")
            }
        }
    }
}

private fun MutableMap<String, JsonElement>.removeAliases(
    namespace: String?,
): Set<Name>? = remove("aliases")?.jsonArray?.map { Name(it.stringPrimitive).withSpaceIfMissing(namespace) }?.toSet()

private fun <K : Any, V : Any> MutableMap<K, V>.removeMandatory(key: K): V {
    return this.remove(key) ?: throw IllegalArgumentException("Missing '$key' key")
}

private fun MutableMap<String, JsonElement>.removeName(): Name {
    val name = removeMandatory("name").stringPrimitive
    val namespace = remove("namespace")?.stringPrimitive
    return Name(name = name, space = namespace)
}

private val JsonElement.stringPrimitive: String
    get() {
        if (this !is JsonPrimitive || isString) throw IllegalArgumentException("Not a string: $this")
        return content
    }

private fun JsonObjectBuilder.putAll(map: Map<String, JsonElement>) {
    map.forEach { (key, value) -> put(key, value) }
}

internal fun AvroSchema.toJsonElement(knownNamedTypes: MutableSet<String> = mutableSetOf()): JsonElement {
    return toJsonElement(null, knownNamedTypes)
}

private fun AvroSchema.toJsonElement(currentSpace: String? = null, knownNamedTypes: MutableSet<String> = mutableSetOf()): JsonElement {
    return when (this) {
        is AvroSchema.PrimitiveSchema, is AvroSchema.NullSchema ->
            if (props.isEmpty()) {
                JsonPrimitive(fullName)
            } else {
                buildJsonObject {
                    put("type", fullName)
                    putAll(props)
                }
            }

        is AvroSchema.UnionSchema -> JsonArray(types.map { it.toJsonElement(currentSpace, knownNamedTypes) })

        is AvroSchema.ArraySchema ->
            buildJsonObject {
                put("type", "array")
                put("items", elementSchema.toJsonElement(currentSpace, knownNamedTypes))
                putAll(props)
            }

        is AvroSchema.MapSchema ->
            buildJsonObject {
                put("type", "map")
                put("values", valueSchema.toJsonElement(currentSpace, knownNamedTypes))
                putAll(props)
            }

        is AvroSchema.NamedSchema -> {
            if (fullName in knownNamedTypes) {
                // Known types are inlined, as they are already defined earlier in the schema, or are provided by the user in another file by example
                JsonPrimitive(if (name.space == currentSpace) name.simpleName else fullName)
            } else {
                knownNamedTypes.add(fullName)
                when (this) {
                    is AvroSchema.EnumSchema ->
                        buildJsonObject {
                            put("type", "enum")
                            putNamedSchemaProps(this@toJsonElement, currentSpace)
                            putJsonArray("symbols") { addAll(symbols) }
                            if (defaultSymbol != null) put("default", defaultSymbol)
                            putAll(props)
                            putAliases(aliases.map { if (name.space == it.space) it.simpleName else it.fullName })
                        }

                    is AvroSchema.FixedSchema ->
                        buildJsonObject {
                            put("type", "fixed")
                            putNamedSchemaProps(this@toJsonElement, currentSpace)
                            put("size", size.toInt())
                            putAll(props)
                            putAliases(aliases.map { if (name.space == it.space) it.simpleName else it.fullName })
                        }

                    is AvroSchema.RecordSchema ->
                        buildJsonObject {
                            put("type", "record")
                            putNamedSchemaProps(this@toJsonElement, currentSpace)
                            putJsonArray("fields") {
                                fields.forEach { field ->
                                    addJsonObject {
                                        put("name", field.name)
                                        put("type", field.schema.toJsonElement(name.space ?: currentSpace, knownNamedTypes))
                                        if (field.doc != null) put("doc", field.doc)
                                        if (field.defaultValue != null) put("default", field.defaultValue)
                                        putAliases(field.aliases)
                                        putAll(field.props)
                                    }
                                }
                            }
                            putAll(props)
                            putAliases(aliases.map { if (name.space == it.space) it.simpleName else it.fullName })
                        }
                }
            }
        }
    }
}

private fun JsonObjectBuilder.putNamedSchemaProps(schema: AvroSchema.NamedSchema, currentSpace: String?) {
    put("name", schema.name.simpleName)
    if (schema.name.space != null) {
        if (schema.name.space != currentSpace) {
            put("namespace", schema.name.space)
        }
    } else if (currentSpace != null) {
        put("namespace", "")
    }
    if (schema.doc != null) put("doc", schema.doc)
}

private fun JsonObjectBuilder.putAliases(aliases: Collection<String>) {
    if (aliases.isNotEmpty()) putJsonArray("aliases") { addAll(aliases) }
}