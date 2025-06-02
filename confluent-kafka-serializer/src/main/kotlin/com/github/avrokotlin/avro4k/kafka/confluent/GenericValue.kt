package com.github.avrokotlin.avro4k.kafka.confluent

import org.apache.avro.Schema
import org.apache.avro.generic.GenericContainer

/**
 * A [GenericContainer] that holds a value and a schema.
 *
 * In root level, it is used to hold a value and a schema to force a specific schema to the schema registry.
 * When used next to a union with ambiguous types, it is used to specify explicitly the schema to use.
 */
public data class GenericValue(private val schema: Schema, public val value: Any?) : GenericContainer {
    override fun getSchema(): Schema = schema
}