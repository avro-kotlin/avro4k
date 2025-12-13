package com.github.avrokotlin.avro4k

import java.io.Serializable

public fun interface NameStrategy : Serializable {
    public fun format(original: String): String

    /**
     * Identifier used for debugging or cache key purposes.
     */
    public val identifier: String
        get() = this::class.java.name ?: "nameStrategy"

    public companion object {
        public val IDENTITY: NameStrategy = NamedNameStrategy("identity") { it }
        public val CAMEL_CASE: NameStrategy = NamedNameStrategy("camelCase") { it.toCamelCase() }
        public val SNAKE_CASE: NameStrategy = NamedNameStrategy("snakeCase") { it.toSnakeCase() }
        public val PASCAL_CASE: NameStrategy = NamedNameStrategy("pascalCase") { it.toPascalCase() }

        public fun custom(identifier: String, formatter: (String) -> String): NameStrategy =
            NamedNameStrategy(identifier, formatter)
    }
}

private data class NamedNameStrategy(
    override val identifier: String,
    val formatter: (String) -> String,
) : NameStrategy {
    override fun format(original: String): String = formatter(original)
}

internal fun String.toPascalCase(): String =
    split(Regex("[\\W_]+"))
        .filter { it.isNotEmpty() }
        .joinToString("") { it.uppercaseFirstChar() }

internal fun String.toCamelCase(): String = toPascalCase().lowercaseFirstChar()

internal fun String.toSnakeCase(): String {
    return trim()
        .replace(Regex("([a-z0-9])([A-Z])"), "$1_$2")
        .replace(Regex("[\\W]+"), "_")
        .replace(Regex("__+"), "_")
        .trim('_')
        .lowercase()
}

internal fun String.uppercaseFirstChar(): String = replaceFirstChar { it.uppercaseChar() }

internal fun String.lowercaseFirstChar(): String = replaceFirstChar { it.lowercaseChar() }
