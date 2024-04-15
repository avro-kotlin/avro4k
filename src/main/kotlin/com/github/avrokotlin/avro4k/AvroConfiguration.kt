package com.github.avrokotlin.avro4k

import com.github.avrokotlin.avro4k.schema.FieldNamingStrategy
import com.github.avrokotlin.avro4k.schema.RecordNamingStrategy

data class AvroConfiguration(
    /**
     * The naming strategy to use for record's name and namespace. Also applied for fixed and enum types.
     *
     * Default: [RecordNamingStrategy.Builtins.FullyQualified]
     */
    val recordNamingStrategy: RecordNamingStrategy = RecordNamingStrategy.Builtins.FullyQualified,
    /**
     * The naming strategy to use for field's name.
     *
     * Default: [FieldNamingStrategy.Builtins.NoOp]
     */
    val fieldNamingStrategy: FieldNamingStrategy = FieldNamingStrategy.Builtins.NoOp,
    /**
     * By default, during decoding, any missing value for a nullable field without default `null` value (e.g. `val field: Type?` without `= null`) is failing.
     * When set to `true`, the nullable fields that haven't any default value are set as null if the value is missing. It also adds `"default": null` to those fields when generating schema using avro4k.
     */
    val implicitNulls: Boolean = false,
)