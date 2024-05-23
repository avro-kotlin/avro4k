package com.github.avrokotlin.avro4k

import com.github.avrokotlin.avro4k.schema.FieldNamingStrategy
import kotlinx.serialization.ExperimentalSerializationApi

public data class AvroConfiguration(
    /**
     * The naming strategy to use for records' fields name.
     *
     * Default: [FieldNamingStrategy.Builtins.NoOp]
     */
    @ExperimentalSerializationApi
    val fieldNamingStrategy: FieldNamingStrategy = FieldNamingStrategy.Builtins.NoOp,
    /**
     * By default, set to `true`, the nullable fields that haven't any default value are set as null if the value is missing. It also adds `"default": null` to those fields when generating schema using avro4k.
     * When set to `false`, during decoding, any missing value for a nullable field without default `null` value (e.g. `val field: Type?` without `= null`) is failing.
     */
    @ExperimentalSerializationApi
    val implicitNulls: Boolean = true,
)