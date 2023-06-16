package com.github.avrokotlin.avro4k

import com.github.avrokotlin.avro4k.schema.DefaultNamingStrategy
import com.github.avrokotlin.avro4k.schema.NamingStrategy

data class AvroConfiguration(
   val namingStrategy: NamingStrategy = DefaultNamingStrategy,
   /**
    * By default, during decoding, any missing value for a nullable field without default [null] value (e.g. `val field: Type?` without `= null`) is failing.
    * When set to [true], the nullable fields that haven't any default value are set as null if the value is missing. It also adds `"default": null` to those fields when generating schema using avro4k.
    */
   val implicitNulls: Boolean = false,
)
