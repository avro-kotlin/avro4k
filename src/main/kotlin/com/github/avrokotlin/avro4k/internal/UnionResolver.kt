package com.github.avrokotlin.avro4k.internal

import org.apache.avro.Schema
import java.util.WeakHashMap

internal class UnionResolver {
    /**
     * For a given union schema, we cache the possible schemas by fullName or alias.
     */
    private val cache: MutableMap<Schema, Map<String, Schema>> = WeakHashMap()

    fun tryResolveUnion(
        schema: Schema,
        typeName: String,
    ): Schema? {
        if (schema.type != Schema.Type.UNION) {
            if (schema.fullName == typeName || schema.isNamedType() && typeName in schema.aliases) {
                return schema
            }
            return null
        }
        return cache.getOrPut(schema) { loadCache(schema) }[typeName]
    }

    private fun loadCache(unionSchema: Schema): MutableMap<String, Schema> {
        val possibleSchemasByNameOrAlias = mutableMapOf<String, Schema>()
        for (type in unionSchema.types) {
            possibleSchemasByNameOrAlias[type.fullName] = type
            if (type.isNamedType()) {
                for (alias in type.aliases) {
                    possibleSchemasByNameOrAlias[alias] = type
                }
            }
        }
        return possibleSchemasByNameOrAlias
    }
}

private fun Schema.isNamedType() = type == Schema.Type.FIXED || type == Schema.Type.ENUM || type == Schema.Type.RECORD