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
import com.github.avrokotlin.avro4k.AvroSchema.NamedSchema
import com.github.avrokotlin.avro4k.AvroSchema.NullSchema
import com.github.avrokotlin.avro4k.AvroSchema.RecordSchema
import com.github.avrokotlin.avro4k.AvroSchema.StringSchema
import com.github.avrokotlin.avro4k.AvroSchema.UnionSchema
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import org.apache.avro.JsonProperties
import org.apache.avro.Schema
import java.nio.charset.StandardCharsets

internal fun AvroSchema.Companion.from(schema: Schema): AvroSchema {
    return from(schema, mutableMapOf())
}

private fun from(schema: Schema, seenNamedTypes: MutableMap<String, NamedSchema>): AvroSchema {
    seenNamedTypes[schema.fullName]?.let {
        return it
    }

    return when (schema.type) {
        Schema.Type.RECORD -> {
            val fields = mutableListOf<RecordSchema.Field>()
            val recordSchema =
                RecordSchema(
                    name = Name(schema.name, schema.namespace),
                    fields = fields,
                    doc = schema.doc,
                    aliases = schema.aliasesWithSpace,
                    props = schema.objectProps.toJsonElementMap()
                )
            seenNamedTypes[schema.fullName] = recordSchema
            schema.fields.map { field ->
                RecordSchema.Field(
                    name = field.name(),
                    schema = from(field.schema(), seenNamedTypes),
                    defaultValue =
                        if (field.hasDefaultValue()) {
                            toJsonElement(field.defaultVal())
                        } else {
                            null
                        },
                    doc = field.doc(),
                    aliases = field.aliases(),
                    props = field.objectProps.toJsonElementMap()
                )
            }.forEach { fields += it }
            recordSchema
        }

        Schema.Type.ENUM ->
            EnumSchema(
                name = Name(schema.name, schema.namespace),
                symbols = schema.enumSymbols.toSet(),
                defaultSymbol = schema.enumDefault,
                doc = schema.doc,
                aliases = schema.aliasesWithSpace,
                props = schema.objectProps.toJsonElementMap()
            ).also { seenNamedTypes[schema.fullName] = it }

        Schema.Type.UNION -> UnionSchema(schema.types.map { from(it, seenNamedTypes) })

        Schema.Type.FIXED ->
            FixedSchema(
                name = Name(schema.name, schema.namespace),
                size = schema.fixedSize.toUInt(),
                doc = schema.doc,
                aliases = schema.aliasesWithSpace,
                props = schema.objectProps.toJsonElementMap()
            ).also { seenNamedTypes[schema.fullName] = it }

        Schema.Type.ARRAY ->
            ArraySchema(
                elementSchema = from(schema.elementType, seenNamedTypes),
                props = schema.objectProps.toJsonElementMap()
            )

        Schema.Type.MAP ->
            MapSchema(
                valueSchema = from(schema.valueType, seenNamedTypes),
                props = schema.objectProps.toJsonElementMap()
            )

        Schema.Type.BOOLEAN -> BooleanSchema(schema.objectProps.toJsonElementMap())
        Schema.Type.INT -> IntSchema(schema.objectProps.toJsonElementMap())
        Schema.Type.LONG -> LongSchema(schema.objectProps.toJsonElementMap())
        Schema.Type.FLOAT -> FloatSchema(schema.objectProps.toJsonElementMap())
        Schema.Type.DOUBLE -> DoubleSchema(schema.objectProps.toJsonElementMap())
        Schema.Type.STRING -> StringSchema(schema.objectProps.toJsonElementMap())
        Schema.Type.BYTES -> BytesSchema(schema.objectProps.toJsonElementMap())
        Schema.Type.NULL -> NullSchema(schema.objectProps.toJsonElementMap())
    }
}

private fun Map<String, Any?>.toJsonElementMap(): Map<String, JsonElement> {
    return mapValues { (_, value) -> toJsonElement(value) }
}

private fun toJsonElement(value: Any?): JsonElement =
    when (value) {
        null, JsonProperties.NULL_VALUE -> JsonNull
        is Boolean -> JsonPrimitive(value)
        is Number -> JsonPrimitive(value)
        is CharSequence -> JsonPrimitive(value.toString())
        is ByteArray -> JsonPrimitive(String(value, StandardCharsets.ISO_8859_1))
        is List<*> -> JsonArray(value.map { toJsonElement(it) })
        is Map<*, *> -> JsonObject(value.entries.associate { it.key as String to toJsonElement(it.value) })
        else -> throw UnsupportedOperationException("unsupported value of type ${value::class}: $value")
    }

private val Schema.aliasesWithSpace: Set<Name>
    get() = aliases.map { Name(it).withSpaceIfMissing(namespace) }.toSet()