@file:OptIn(ExperimentalSerializationApi::class)

package com.github.avrokotlin.avro4k

import com.github.avrokotlin.avro4k.AvroSchema.ArraySchema
import com.github.avrokotlin.avro4k.AvroSchema.BooleanSchema
import com.github.avrokotlin.avro4k.AvroSchema.BytesSchema
import com.github.avrokotlin.avro4k.AvroSchema.DoubleSchema
import com.github.avrokotlin.avro4k.AvroSchema.EnumSchema
import com.github.avrokotlin.avro4k.AvroSchema.FixedSchema
import com.github.avrokotlin.avro4k.AvroSchema.FloatSchema
import com.github.avrokotlin.avro4k.AvroSchema.IntSchema
import com.github.avrokotlin.avro4k.AvroSchema.LongSchema
import com.github.avrokotlin.avro4k.AvroSchema.MapSchema
import com.github.avrokotlin.avro4k.AvroSchema.NullSchema
import com.github.avrokotlin.avro4k.AvroSchema.RecordSchema
import com.github.avrokotlin.avro4k.AvroSchema.StringSchema
import com.github.avrokotlin.avro4k.AvroSchema.UnionSchema
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.floatOrNull
import kotlinx.serialization.json.int
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.longOrNull

internal fun AvroSchema.Companion.fromJsonString(
    json: String,
    knownNamedTypes: MutableMap<String, AvroSchema> = mutableMapOf(),
): AvroSchema {
    return fromJsonElement(Json.parseToJsonElement(json), null, knownNamedTypes)
}

public fun AvroSchema.Companion.fromJsonElement(
    element: JsonElement,
    knownNamedTypes: MutableMap<String, AvroSchema> = mutableMapOf(),
): AvroSchema {
    return fromJsonElement(element, null, knownNamedTypes)
}

private fun fromJsonElement(
    element: JsonElement,
    currentSpace: String?,
    knownNamedTypes: MutableMap<String, AvroSchema>,
): AvroSchema {
    return when (element) {
        is JsonPrimitive -> {
            // Handle shorthand primitive type or referenced named type
            when (val type = element.contentOrNull) {
                "null" -> NullSchema()

                "boolean" -> BooleanSchema()

                "int" -> IntSchema()

                "long" -> LongSchema()

                "float" -> FloatSchema()

                "double" -> DoubleSchema()

                "bytes" -> BytesSchema()

                "string" -> StringSchema()

                null -> throw IllegalArgumentException("type cannot be null json literal")

                else ->
                    knownNamedTypes[Name(type, currentSpace).fullName]
                        ?: throw IllegalArgumentException("Unknown named type: $type")
            }
        }

        is JsonArray -> {
            // Handle union
            UnionSchema(element.map {
                val type = fromJsonElement(it, currentSpace, knownNamedTypes)
                if (type !is ResolvedSchema) {
                    throw IllegalArgumentException("Unions cannot contain nested unions")
                }
                type
            })
        }

        is JsonObject -> {
            val props = element.toMutableMap()
            when (val type = props.removeMandatory("type").stringPrimitive) {
                "null" -> NullSchema(props = props)

                "boolean" -> BooleanSchema(props = props)

                "int" -> IntSchema(props = props)

                "long" -> LongSchema(props = props)

                "float" -> FloatSchema(props = props)

                "double" -> DoubleSchema(props = props)

                "bytes" -> BytesSchema(props = props)

                "string" -> StringSchema(props = props)

                "map" ->
                    MapSchema(
                        valueSchema = fromJsonElement(props.removeMandatory("values"), currentSpace, knownNamedTypes),
                        props = props
                    )

                "array" ->
                    ArraySchema(
                        elementSchema = fromJsonElement(props.removeMandatory("items"), currentSpace, knownNamedTypes),
                        props = props
                    )

                "enum" -> {
                    val name = props.removeName(currentSpace)
                    EnumSchema(
                        name = name,
                        symbols = props.removeMandatory("symbols").jsonArray.map { it.stringPrimitive },
                        defaultSymbol = props.remove("default")?.stringPrimitive,
                        doc = props.remove("doc")?.stringPrimitive,
                        aliases = props.removeAliases(name.space) ?: emptySet(),
                        props = props
                    ).also { knownNamedTypes[name.fullName] = it }
                }

                "fixed" -> {
                    val name = props.removeName(currentSpace)
                    FixedSchema(
                        name = name,
                        size = props.removeMandatory("size").jsonPrimitive.int.toUInt(),
                        doc = props.remove("doc")?.stringPrimitive,
                        aliases = props.removeAliases(name.space) ?: emptySet(),
                        props = props
                    ).also { knownNamedTypes[name.fullName] = it }
                }

                "record" -> {
                    val name = props.removeName(currentSpace)
                    val fieldsJson = props.removeMandatory("fields").jsonArray
                    val fields = LockableList<RecordSchema.Field>()
                    val schema = RecordSchema(
                        name = name,
                        fields = fields,
                        doc = props.remove("doc")?.stringPrimitive,
                        aliases = props.removeAliases(name.space) ?: emptySet(),
                        props = props
                    )
                    knownNamedTypes[name.fullName] = schema
                    fieldsJson.forEach { field ->
                        val fieldProps = field.jsonObject.toMutableMap()
                        fields += RecordSchema.Field(
                            name = fieldProps.removeMandatory("name").stringPrimitive,
                            schema = fromJsonElement(fieldProps.removeMandatory("type"), name.space, knownNamedTypes),
                            doc = fieldProps.remove("doc")?.stringPrimitive,
                            aliases = fieldProps.remove("aliases")?.jsonArray?.map { it.stringPrimitive }?.toSet() ?: emptySet(),
                            // TODO validate record's default, requiring all the fields to be present, and fixed default to be of expected size
                            defaultValue = fieldProps.remove("default"),
                            props = fieldProps
                        )
                    }
                    fields.lock()
                    schema
                }

                else -> throw IllegalArgumentException("Unknown schema type: $type")
            }
        }
    }
}

private fun MutableMap<String, JsonElement>.removeAliases(
    namespace: String?,
): Set<Name>? = remove("aliases")?.jsonArray?.map { Name(it.stringPrimitive, namespace) }?.toSet()

private fun <K : Any, V : Any> MutableMap<K, V>.removeMandatory(key: K): V {
    return this.remove(key) ?: throw IllegalArgumentException("Missing '$key' key")
}

private fun MutableMap<String, JsonElement>.removeName(defaultSpace: String?): Name {
    val name = removeMandatory("name").stringPrimitive
    val namespace = remove("namespace")?.stringPrimitive
    return Name(name = name, space = namespace ?: defaultSpace)
}

private val JsonElement.stringPrimitive: String
    get() {
        if (this !is JsonPrimitive || !isString) throw IllegalArgumentException("Not a string: $this")
        return content
    }

internal fun JsonElement.isValidJsonForSchema(schema: AvroSchema): Boolean {
    // logical type is not taken into account, as the spec indicates
    // that the default value should be of the raw type.
    return when (schema) {
        is UnionSchema -> schema.types.any { isValidJsonForSchema(it) }

        is EnumSchema,
        is FixedSchema,
        is BytesSchema,
        is StringSchema -> this is JsonPrimitive && this.isString

        is NullSchema -> this == JsonNull
        is BooleanSchema -> this is JsonPrimitive && this.booleanOrNull != null
        is IntSchema -> this is JsonPrimitive && this.intOrNull != null
        is LongSchema -> this is JsonPrimitive && this.longOrNull != null
        is FloatSchema -> this is JsonPrimitive && this.floatOrNull != null
        is DoubleSchema -> this is JsonPrimitive && this.doubleOrNull != null
        is ArraySchema -> this is JsonArray && this.all { it.isValidJsonForSchema(schema.elementSchema) }
        is MapSchema -> this is JsonObject && this.all { it.value.isValidJsonForSchema(schema.valueSchema) }
        is RecordSchema -> {
            if (this !is JsonObject) false
            else schema.fields.all { field ->
                val fieldValue = this[field.name] ?: field.defaultValue
                fieldValue != null && fieldValue.isValidJsonForSchema(field.schema)
            }
        }
    }
}