package com.github.avrokotlin.avro4k.plugin.gradle

/**
 * Enum representing the available naming strategies for the Gradle plugin,
 * used to map the original record fields names to the kotlin properties names during source generation.
 */
public enum class FieldNamingStrategyType {
    /**
     * Naming strategy that is basically returning the original avro field name.
     *
     * This is the default strategy.
     */
    IDENTITY,

    /**
     * Naming strategy that formats the original avro field name to its camel-case counterpart.
     *
     * Example: `user_v1UUID` is formatted as `userV1Uuid`.
     */
    CAMEL_CASE,
}