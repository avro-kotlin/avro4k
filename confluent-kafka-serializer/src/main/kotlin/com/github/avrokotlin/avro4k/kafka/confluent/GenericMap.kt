package com.github.avrokotlin.avro4k.kafka.confluent

import org.apache.avro.Schema
import org.apache.avro.generic.GenericContainer

/**
 * A generic container for a map with a schema. Native avro normally accept only string keys,
 * while avro4k allows almost any type of key, the reason why this class allows any type of key.
 * This class is needed only when serializing a map at the root level.
 *
 * @property schema the schema of the map (must be of type MAP)
 */
public class GenericMap<K, V>(private val schema: Schema, private val map: Map<K, V>) : GenericContainer, Map<K, V> by map {
    init {
        require(schema.type == Schema.Type.MAP) { "Schema must be of type MAP" }
    }

    override fun getSchema(): Schema = schema
}