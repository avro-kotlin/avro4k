package com.github.avrokotlin.avro4k.plugin.gradle

import com.github.avrokotlin.avro4k.FieldNamingStrategy

/**
 * Enum representing the available field naming strategies for the Gradle plugin.
 */
public enum class FieldNamingStrategyType {
    IDENTITY,
    CAMEL_CASE,
    ;

    internal fun toGeneratorStrategy(): FieldNamingStrategy =
        when (this) {
            IDENTITY -> FieldNamingStrategy.Identity
            CAMEL_CASE -> FieldNamingStrategy.CamelCase
        }
}