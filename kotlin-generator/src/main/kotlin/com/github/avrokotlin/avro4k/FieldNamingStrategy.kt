package com.github.avrokotlin.avro4k

/**
 * Strategy for converting Avro field names to Kotlin property names.
 * Only applies to record fields, not to class/enum/union names.
 */
public interface FieldNamingStrategy {
    public fun format(original: String): String

    public data object Identity : FieldNamingStrategy {
        override fun format(original: String): String = original
    }

    public data object CamelCase : FieldNamingStrategy {
        override fun format(original: String): String = splitWords(original).joinToCamelCase()
    }
}

private fun splitWords(input: String): List<String> {
    val words = mutableListOf<String>()
    val current = StringBuilder()
    var upperCount = 0

    fun flush() {
        if (current.isNotEmpty()) {
            words.add(current.toString())
            current.clear()
        }
        upperCount = 0
    }

    for (i in input.indices) {
        val c = input[i]
        when {
            c == '_' || c == '-' || !c.isLetterOrDigit() -> flush()
            c.isUpperCase() -> {
                val nextIsLower = i + 1 < input.length && input[i + 1].isLowerCase()
                if (upperCount == 0 && current.isNotEmpty()) {
                    // camelCase boundary: "myXml" -> ["my", "Xml"]
                    flush()
                } else if (upperCount > 0 && nextIsLower) {
                    // acronym end: "XMLParser" -> ["XML", "Parser"]
                    flush()
                }
                current.append(c)
                upperCount++
            }
            else -> {
                current.append(c)
                upperCount = 0
            }
        }
    }
    flush()
    return words
}

private fun List<String>.joinToPascalCase(): String =
    joinToString("") { it.lowercase().replaceFirstChar { c -> c.uppercaseChar() } }

private fun List<String>.joinToCamelCase(): String =
    mapIndexed { index, word ->
        if (index == 0) word.lowercase() else word.lowercase().replaceFirstChar { it.uppercaseChar() }
    }.joinToString("")

internal fun String.toPascalCase(): String = splitWords(this).joinToPascalCase()