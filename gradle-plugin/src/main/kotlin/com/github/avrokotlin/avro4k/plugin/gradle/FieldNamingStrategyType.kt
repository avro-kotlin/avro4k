package com.github.avrokotlin.avro4k.plugin.gradle

/**
 * Enum representing the available naming strategies for the Gradle plugin,
 * used to map the original record fields names to the kotlin properties names during source generation.
 */
public enum class FieldNamingStrategyType {
    IDENTITY,
    CAMEL_CASE,
}