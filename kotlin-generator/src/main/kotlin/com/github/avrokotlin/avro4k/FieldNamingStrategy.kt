package com.github.avrokotlin.avro4k

/**
 * Strategy for converting Avro field names to Kotlin property names.
 * Only applies to record fields, not to class/enum/union names.
 */
public sealed interface FieldNamingStrategy {
    public fun format(original: String): String

    public data object Identity : FieldNamingStrategy {
        override fun format(original: String): String = original
    }

    public data object CamelCase : FieldNamingStrategy {
        override fun format(original: String): String = splitWords(original).joinToCamelCase()
    }

    public data object SnakeCase : FieldNamingStrategy {
        override fun format(original: String): String = splitWords(original).joinToSnakeCase()
    }

    public data object PascalCase : FieldNamingStrategy {
        override fun format(original: String): String = splitWords(original).joinToPascalCase()
    }

    public companion object {
        public val IDENTITY: FieldNamingStrategy = Identity
        public val CAMEL_CASE: FieldNamingStrategy = CamelCase
        public val SNAKE_CASE: FieldNamingStrategy = SnakeCase
        public val PASCAL_CASE: FieldNamingStrategy = PascalCase
    }
}

private fun splitWords(input: String): List<String> {
    val words = mutableListOf<String>()
    val current = StringBuilder()

    for (i in input.indices) {
        val c = input[i]
        when {
            c == '_' || c == '-' || !c.isLetterOrDigit() -> {
                if (current.isNotEmpty()) {
                    words.add(current.toString())
                    current.clear()
                }
            }
            c.isUpperCase() && current.isNotEmpty() && !current.last().isUpperCase() -> {
                words.add(current.toString())
                current.clear()
                current.append(c)
            }
            c.isUpperCase() && current.isNotEmpty() && current.last().isUpperCase() -> {
                val nextIndex = i + 1
                if (nextIndex < input.length && input[nextIndex].isLowerCase()) {
                    words.add(current.toString())
                    current.clear()
                }
                current.append(c)
            }
            else -> current.append(c)
        }
    }
    if (current.isNotEmpty()) {
        words.add(current.toString())
    }
    return words.filter { it.isNotEmpty() }
}

private fun List<String>.joinToPascalCase(): String =
    joinToString("") { it.lowercase().replaceFirstChar { c -> c.uppercaseChar() } }

private fun List<String>.joinToCamelCase(): String =
    mapIndexed { index, word ->
        if (index == 0) word.lowercase() else word.lowercase().replaceFirstChar { it.uppercaseChar() }
    }.joinToString("")

private fun List<String>.joinToSnakeCase(): String =
    joinToString("_") { it.lowercase() }

internal fun String.toPascalCase(): String = splitWords(this).joinToPascalCase()