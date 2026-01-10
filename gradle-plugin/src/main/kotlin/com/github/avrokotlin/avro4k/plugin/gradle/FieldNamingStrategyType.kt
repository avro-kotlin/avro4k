package com.github.avrokotlin.avro4k.plugin.gradle

import com.github.avrokotlin.avro4k.FieldNamingStrategy

/**
 * Enum representing the available field naming strategies for the Gradle plugin.
 */
public enum class FieldNamingStrategyType {
    IDENTITY,
    CAMEL_CASE,
    SNAKE_CASE,
    PASCAL_CASE,
    ;

    internal fun toGeneratorStrategy(): FieldNamingStrategy =
        when (this) {
            IDENTITY -> FieldNamingStrategy.Identity
            CAMEL_CASE -> FieldNamingStrategy.CamelCase
            SNAKE_CASE -> FieldNamingStrategy.SnakeCase
            PASCAL_CASE -> FieldNamingStrategy.PascalCase
        }
}