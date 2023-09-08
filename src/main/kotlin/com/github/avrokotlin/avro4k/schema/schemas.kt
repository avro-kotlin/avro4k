package com.github.avrokotlin.avro4k.schema

import org.apache.avro.Schema

// creates a union schema type, with nested unions extracted, and duplicate nulls stripped
// union schemas can't contain other union schemas as a direct
// child, so whenever we create a union, we need to check if our
// children are unions and flatten
fun createSafeUnion(nullFirst : Boolean,vararg schemas: Schema): Schema {
   val flattened = schemas.flatMap { schema -> runCatching { schema.types }.getOrElse { listOf(schema) } }
   val (nulls, rest) = flattened.partition { it.type == Schema.Type.NULL }
   return Schema.createUnion(if(nullFirst) nulls + rest else rest + nulls)
}

fun Schema.extractNonNull(): Schema = nonNullSchema
val Schema.nonNullSchema : Schema
   get() = if(type == Schema.Type.UNION) {
      //Writing the simple "nullable" case explicitly for faster performance
      if(types.size == 2) {
         if (types[0].type == Schema.Type.NULL) {
            types[1]
         } else if (types[1].type == Schema.Type.NULL) {
            types[0]
         } else {
            this
         }
      } else {
         this.types.filter { it.type != Schema.Type.NULL }.let { if(it.size > 1) Schema.createUnion(it) else it[0] }
      }
   } else {
      this
   }
val Schema.nonNullSchemaIndex : Int
   get() {
      //Writing the simple "nullable" case explicitly for faster performance
      return if(type == Schema.Type.UNION && types.size == 2) {
         var nonNullSchemaIndex = -1
         if (types[0].type == Schema.Type.NULL) {
              nonNullSchemaIndex = 1
         } else if (types[1].type == Schema.Type.NULL) {
              nonNullSchemaIndex = 0
         }
         nonNullSchemaIndex
      } else {
         -1
      }
   }      
val Schema.nullSchemaIndex : Int 
   get() = if(type == Schema.Type.UNION) {
      //Writing the simple "nullable" case explicitly for faster performance
      if(types.size == 2) {
         var nullSchemaIndex = -1
         if (types[0].type == Schema.Type.NULL) {
            nullSchemaIndex = 0
         } else if (types[1].type == Schema.Type.NULL) {
            nullSchemaIndex = 1
         }
         nullSchemaIndex
      } else {
         this.types.indexOfFirst { it.type == Schema.Type.NULL }
      }
   } else {
      -1
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
      Schema.Type.ENUM -> Schema.createEnum(name, doc, namespace, enumSymbols, enumDefault)
      Schema.Type.FIXED -> Schema.createFixed(name, doc, namespace, fixedSize)
      Schema.Type.MAP -> Schema.createMap(valueType.overrideNamespace(namespace))
      Schema.Type.ARRAY -> Schema.createArray(elementType.overrideNamespace(namespace))
      else -> this
   }
}