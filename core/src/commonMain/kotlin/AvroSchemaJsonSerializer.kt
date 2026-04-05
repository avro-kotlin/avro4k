@file:OptIn(ExperimentalSerializationApi::class)

package com.github.avrokotlin.avro4k

import com.github.avrokotlin.avro4k.AvroSchema.ArraySchema
import com.github.avrokotlin.avro4k.AvroSchema.EnumSchema
import com.github.avrokotlin.avro4k.AvroSchema.FixedSchema
import com.github.avrokotlin.avro4k.AvroSchema.MapSchema
import com.github.avrokotlin.avro4k.AvroSchema.NamedSchema
import com.github.avrokotlin.avro4k.AvroSchema.NullSchema
import com.github.avrokotlin.avro4k.AvroSchema.PrimitiveSchema
import com.github.avrokotlin.avro4k.AvroSchema.RecordSchema
import com.github.avrokotlin.avro4k.AvroSchema.UnionSchema
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObjectBuilder
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.addAll
import kotlinx.serialization.json.addJsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray

public fun AvroSchema.toJsonElement(knownNamedTypes: MutableSet<String> = mutableSetOf()): JsonElement {
    return toJsonElement(null, knownNamedTypes)
}

private fun AvroSchema.toJsonElement(currentSpace: String? = null, knownNamedTypes: MutableSet<String> = mutableSetOf()): JsonElement {
    return when (this) {
        is PrimitiveSchema, is NullSchema ->
            if (props.isEmpty()) {
                JsonPrimitive(fullName)
            } else {
                buildJsonObject {
                    put("type", fullName)
                    putAll(props)
                }
            }

        is UnionSchema -> JsonArray(types.map { it.toJsonElement(currentSpace, knownNamedTypes) })

        is ArraySchema ->
            buildJsonObject {
                put("type", "array")
                put("items", elementSchema.toJsonElement(currentSpace, knownNamedTypes))
                putAll(props)
            }

        is MapSchema ->
            buildJsonObject {
                put("type", "map")
                put("values", valueSchema.toJsonElement(currentSpace, knownNamedTypes))
                putAll(props)
            }

        is NamedSchema -> {
            if (fullName in knownNamedTypes) {
                // Known types are inlined, as they are already defined earlier in the schema, or are provided by the user in another file by example
                JsonPrimitive(if (name.space == currentSpace) name.simpleName else fullName)
            } else {
                knownNamedTypes.add(fullName)
                when (this) {
                    is EnumSchema ->
                        buildJsonObject {
                            put("type", "enum")
                            putNamedSchemaProps(this@toJsonElement, currentSpace)
                            putJsonArray("symbols") { addAll(symbols) }
                            if (defaultSymbol != null) put("default", defaultSymbol)
                            putAll(props)
                            putAliases(aliases.map { if (name.space == it.space) it.simpleName else it.fullName })
                        }

                    is FixedSchema ->
                        buildJsonObject {
                            put("type", "fixed")
                            putNamedSchemaProps(this@toJsonElement, currentSpace)
                            put("size", size.toInt())
                            putAll(props)
                            putAliases(aliases.map { if (name.space == it.space) it.simpleName else it.fullName })
                        }

                    is RecordSchema ->
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

private fun JsonObjectBuilder.putNamedSchemaProps(schema: NamedSchema, currentSpace: String?) {
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

private fun JsonObjectBuilder.putAll(map: Map<String, JsonElement>) {
    map.forEach { (key, value) -> put(key, value) }
}