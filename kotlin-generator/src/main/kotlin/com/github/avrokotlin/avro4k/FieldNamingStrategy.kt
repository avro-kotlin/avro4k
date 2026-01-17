package com.github.avrokotlin.avro4k

/**
 * Strategy for converting Avro field names to Kotlin property names.
 * Only applies to record fields, not to class/enum/union names.
 */
@InternalAvro4kApi
public interface FieldNamingStrategy {
    public fun format(original: String): String

    @InternalAvro4kApi
    public data object Identity : FieldNamingStrategy {
        override fun format(original: String): String = original
    }

    @InternalAvro4kApi
    public data object CamelCase : FieldNamingStrategy {
        override fun format(original: String): String = toCamelCase(original)
    }
}

private fun toCamelCase(input: String, doUppercaseFirstChar: Boolean = false) =
    StringBuilder(input.length).apply {
        var wordStart = -1

        fun startWord(startInclusive: Int) {
            wordStart = startInclusive
        }

        fun isWordStarted(): Boolean {
            return wordStart != -1
        }

        fun endWord(endExclusive: Int) {
            if (!doUppercaseFirstChar && isEmpty()) {
                append(input.substring(wordStart, endExclusive).lowercase())
            } else {
                append(input[wordStart].uppercaseChar())
                append(input.substring(wordStart + 1, endExclusive).lowercase())
            }
            wordStart = -1
        }

        for (i in input.indices) {
            val c = input[i]
            when {
                !c.isLetterOrDigit() -> {
                    if (isWordStarted()) {
                        endWord(i)
                    }
                }

                !isWordStarted() -> startWord(i)

                !c.isDigit() && input[i - 1].isDigit() -> {
                    // "abc123" -> ["abc", "123"]
                    endWord(i)
                    startWord(i)
                }

                c.isUpperCase() -> {
                    if (!input[i - 1].isUpperCase()) {
                        // "myXml" -> ["my", "Xml"]
                        endWord(i)
                        startWord(i)
                    } else if (i + 1 < input.length && input[i + 1].isLowerCase()) {
                        // acronym present: "testXMLParser" -> ["test", "XML", "Parser"]
                        endWord(i)
                        startWord(i)
                    }
                }
            }
        }

        if (isWordStarted()) {
            endWord(input.length)
        }
        if (isEmpty()) {
            throw IllegalArgumentException("Input string does not contain any valid letter or digit (a-zA-Z0-9)")
        }
    }.toString()

internal fun String.toPascalCase(): String = toCamelCase(this, doUppercaseFirstChar = true)