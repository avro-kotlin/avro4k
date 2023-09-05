package com.github.avrokotlin.avro4k

import com.github.avrokotlin.avro4k.schema.DefaultNamingStrategy
import com.github.avrokotlin.avro4k.schema.NamingStrategy

data class AvroConfiguration(
    /**
     * Only applied to field names and fixed type names
     */
    val namingStrategy: NamingStrategy = DefaultNamingStrategy,
    /**
     * By default, during decoding, any missing value for a nullable field without default [null] value (e.g. `val field: Type?` without `= null`) is failing.
     * When set to [true], the nullable fields that haven't any default value are set as null if the value is missing. It also adds `"default": null` to those fields when generating schema using avro4k.
     */
    val implicitNulls: Boolean = false,
    /**
     * When encoding, if a kotlin field is encoded while it doesn't exist inside the schema, or a schema field is decoded while it doesn't exist inside the kotlin class, you can define what to do:
     * - true (default): Just ignore the kotlin field and encode/decode the next field, as [kotlinx.serialization.Transient] is doing
     * - false: Fails encoding with a [kotlinx.serialization.SerializationException]
     */
    val ignoreUnknownFields: Boolean = true
)