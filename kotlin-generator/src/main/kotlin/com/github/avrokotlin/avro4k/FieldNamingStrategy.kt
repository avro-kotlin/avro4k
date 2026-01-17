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
    var wordStart = -1

    fun flush(endExclusive: Int) {
        if (wordStart >= 0) {
            words.add(input.substring(wordStart, endExclusive))
            wordStart = -1
        }
    }

    for (i in input.indices) {
        val c = input[i]
        when {
            c == '_' || c == '-' || !c.isLetterOrDigit() -> flush(i)
            c.isLetter() && i > 0 && input[i - 1].isDigit() -> {
                flush(i)
                wordStart = i
            }
            c.isUpperCase() -> {
                if (wordStart == -1) {
                    wordStart = i
                } else {
                    val prevIsUpper = input[i - 1].isUpperCase()
                    if (!prevIsUpper) {
                        // camelCase boundary: "myXml" -> ["my", "Xml"]
                        flush(i)
                        wordStart = i
                    } else {
                        val nextIsLower = i + 1 < input.length && input[i + 1].isLowerCase()
                        if (nextIsLower) {
                            // acronym end: "XMLParser" -> ["XML", "Parser"]
                            flush(i)
                            wordStart = i
                        }
                    }
                }
            }
            else ->
                if (wordStart == -1) {
                    wordStart = i
                }
        }
    }

    if (wordStart >= 0) {
        words.add(input.substring(wordStart))
    }
    return words
}

private fun List<String>.joinToPascalCase(): String =
    joinToString("") { it.lowercase().replaceFirstChar { c -> c.uppercaseChar() } }

private fun List<String>.joinToCamelCase(): String =
    mapIndexed { index, word ->
        if (index == 0) word.lowercase() else word.lowercase().replaceFirstChar { it.uppercaseChar() }
    }.joinToString("")

internal fun String.toPascalCase(): String = splitWords(this).joinToPascalCase()