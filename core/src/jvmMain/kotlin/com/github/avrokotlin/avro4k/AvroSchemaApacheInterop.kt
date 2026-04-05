package com.github.avrokotlin.avro4k

import com.github.avrokotlin.avro4k.internal.WeakIdentityKeyCache
import kotlinx.serialization.json.Json
import org.apache.avro.NameValidator
import org.apache.avro.Schema
import org.apache.avro.SchemaFormatter

private val newToApacheSchemaCache = WeakIdentityKeyCache<AvroSchema, Schema>()
private val apacheToNewSchemaCache = WeakIdentityKeyCache<Schema, AvroSchema>()

public fun AvroSchema.Companion.fromApacheSchema(schema: Schema): AvroSchema =
    apacheToNewSchemaCache.getOrPut(schema) {
        AvroSchema.fromJsonString(SchemaFormatter.format("json", schema))
    }

public fun AvroSchema.toApacheSchema(): Schema = newToApacheSchemaCache.getOrPut(this) {
    val json = toJsonElement()
    Schema.Parser(NameValidator.NO_VALIDATION).parse(Json.encodeToString(json))
}
