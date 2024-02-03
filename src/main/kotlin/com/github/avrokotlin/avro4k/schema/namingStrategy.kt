package com.github.avrokotlin.avro4k.schema

import kotlinx.serialization.descriptors.SerialDescriptor

interface RecordNamingStrategy {
    fun resolve(
        descriptor: SerialDescriptor,
        serialName: String,
    ): RecordName

    companion object Builtins {
        /**
         * Extract the record name from the fully qualified class name by taking the last part of the class name as the record name and the rest as the namespace.
         *
         * If there is no dot, then the namespace is null.
         */
        object FullyQualified : RecordNamingStrategy {
            override fun resolve(
                descriptor: SerialDescriptor,
                serialName: String,
            ): RecordName {
                val lastDot = serialName.lastIndexOf('.').takeIf { it >= 0 && it + 1 < serialName.length }
                val lastIndex = if (serialName.endsWith('?')) serialName.length - 1 else serialName.length
                return RecordName(
                    name = lastDot?.let { serialName.substring(lastDot + 1, lastIndex) } ?: serialName,
                    namespace = lastDot?.let { serialName.substring(0, lastDot) }?.takeIf { it.isNotEmpty() }
                )
            }
        }
    }
}

data class RecordName(val name: String, val namespace: String?)

interface FieldNamingStrategy {
    fun resolve(
        descriptor: SerialDescriptor,
        elementIndex: Int,
        serialName: String,
    ): String

    companion object Builtins {
        /**
         * Returns the field name as is.
         */
        object NoOp : FieldNamingStrategy {
            override fun resolve(
                descriptor: SerialDescriptor,
                elementIndex: Int,
                serialName: String,
            ) = serialName
        }

        /**
         * Convert the field name to snake_case by adding an underscore before each capital letter, and lowercase those capital letters.
         */
        object SnakeCase : FieldNamingStrategy {
            override fun resolve(
                descriptor: SerialDescriptor,
                elementIndex: Int,
                serialName: String,
            ): String =
                buildString(serialName.length * 2) {
                    var bufferedChar: Char? = null
                    var previousUpperCharsCount = 0

                    serialName.forEach { c ->
                        if (c.isUpperCase()) {
                            if (previousUpperCharsCount == 0 && isNotEmpty() && last() != '_') {
                                append('_')
                            }

                            bufferedChar?.let(::append)

                            previousUpperCharsCount++
                            bufferedChar = c.lowercaseChar()
                        } else {
                            if (bufferedChar != null) {
                                if (previousUpperCharsCount > 1 && c.isLetter()) {
                                    append('_')
                                }
                                append(bufferedChar)
                                previousUpperCharsCount = 0
                                bufferedChar = null
                            }
                            append(c)
                        }
                    }

                    if (bufferedChar != null) {
                        append(bufferedChar)
                    }
                }
        }

        /**
         * Enforce camelCase naming strategy by upper-casing the first field name letter.
         */
        object PascalCase : FieldNamingStrategy {
            override fun resolve(
                descriptor: SerialDescriptor,
                elementIndex: Int,
                serialName: String,
            ): String = serialName.replaceFirstChar { it.uppercaseChar() }
        }

        /**
         * Enforce camelCase naming strategy by lower-casing the first field name letter.
         */
        object CamelCase : FieldNamingStrategy {
            override fun resolve(
                descriptor: SerialDescriptor,
                elementIndex: Int,
                serialName: String,
            ): String = serialName.replaceFirstChar { it.lowercaseChar() }
        }
    }
}