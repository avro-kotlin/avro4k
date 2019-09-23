package com.sksamuel.avro4k

import org.apache.avro.Schema

object SchemaHelper {

  // creates a union schema type, with nested unions extracted, and duplicate nulls stripped
  // union schemas can't contain other union schemas as a direct
  // child, so whenever we create a union, we need to check if our
  // children are unions and flatten
  fun createSafeUnion(vararg schemas: Schema): Schema {
    val flattened = schemas.flatMap { schema -> kotlin.runCatching { schema.types }.getOrElse { listOf(schema) } }
    val (nulls, rest) = flattened.partition { it.type == Schema.Type.NULL }
    return Schema.createUnion(nulls + rest)
  }

  fun findSubschema(schema: Schema, name: String): Schema? {
    require(schema.type == Schema.Type.RECORD)
    return schema.fields.find { it.name() == name }?.schema()
  }
}
