package com.github.avrokotlin.avro4k.schema

import kotlinx.serialization.descriptors.SerialDescriptor

interface RecordNamingStrategy {
    fun resolve(
        descriptor: SerialDescriptor,
        serialName: String,
    ): RecordName

    companion object Builtins {
        val Default =
            object : RecordNamingStrategy {
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
        val Default: FieldNamingStrategy =
            object : FieldNamingStrategy {
                override fun resolve(
                    descriptor: SerialDescriptor,
                    elementIndex: Int,
                    serialName: String,
                ) = serialName
            }
        val SnakeCase: FieldNamingStrategy =
            object : FieldNamingStrategy {
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
        val PascalCase: FieldNamingStrategy =
            object : FieldNamingStrategy {
                override fun resolve(
                    descriptor: SerialDescriptor,
                    elementIndex: Int,
                    serialName: String,
                ): String = serialName.replaceFirstChar { it.uppercaseChar() }
            }
        val CamelCase: FieldNamingStrategy =
            object : FieldNamingStrategy {
                override fun resolve(
                    descriptor: SerialDescriptor,
                    elementIndex: Int,
                    serialName: String,
                ): String = serialName.replaceFirstChar { it.lowercaseChar() }
            }
    }
}