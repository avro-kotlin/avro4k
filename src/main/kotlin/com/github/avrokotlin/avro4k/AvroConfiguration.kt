package com.github.avrokotlin.avro4k

import com.github.avrokotlin.avro4k.schema.FieldNamingStrategy
import com.github.avrokotlin.avro4k.schema.TypeNamingStrategy

data class AvroConfiguration(
    /**
     * The naming strategy to use for complex types (record, enum and fixed types).
     *
     * Default: [TypeNamingStrategy.Builtins.FullyQualified]
     */
    val typeNamingStrategy: TypeNamingStrategy = TypeNamingStrategy.Builtins.FullyQualified,
    /**
     * The naming strategy to use for records' fields name.
     *
     * Default: [FieldNamingStrategy.Builtins.NoOp]
     */
    val fieldNamingStrategy: FieldNamingStrategy = FieldNamingStrategy.Builtins.NoOp,
    /**
     * By default, set to `true`, the nullable fields that haven't any default value are set as null if the value is missing. It also adds `"default": null` to those fields when generating schema using avro4k.
     * When set to `false`, during decoding, any missing value for a nullable field without default `null` value (e.g. `val field: Type?` without `= null`) is failing.
     */
    val implicitNulls: Boolean = true,
    /**
     * The encoding format to use when encoding and decoding avro data. Default is [EncodedAs.BINARY].
     *
     * @see EncodedAs
     */
    val encodedAs: EncodedAs = EncodedAs.BINARY,
)

enum class EncodedAs {
    BINARY,
    JSON_COMPACT,
    JSON_PRETTY,
}