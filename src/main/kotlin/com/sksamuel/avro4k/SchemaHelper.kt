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

fun Schema.containsNull(): Boolean =
   type == Schema.Type.UNION && types.any { it.type == Schema.Type.NULL }

fun Schema.extractNonNull(): Schema = when (this.type) {
   Schema.Type.UNION -> this.types.first { it.type != Schema.Type.NULL }
   else -> this
}

/**
 * Takes an Avro schema, and overrides the namespace of that schema with the given namespace.
 */
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
      Schema.Type.ENUM -> Schema.createEnum(name, doc, namespace, enumSymbols)
      Schema.Type.FIXED -> Schema.createFixed(name, doc, namespace, fixedSize)
      Schema.Type.MAP -> Schema.createMap(valueType.overrideNamespace(namespace))
      Schema.Type.ARRAY -> Schema.createArray(elementType.overrideNamespace(namespace))
      else -> this
   }
}