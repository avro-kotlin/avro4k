package com.github.avrokotlin.avro4k

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor

public data class AvroConfiguration(
    /**
     * The naming strategy to use for records' fields name.
     *
     * Default: [FieldNamingStrategy.Builtins.OriginalElementName]
     */
    @ExperimentalSerializationApi
    val fieldNamingStrategy: FieldNamingStrategy = FieldNamingStrategy.Builtins.OriginalElementName,
    /**
     * By default, set to `true`, the nullable fields that haven't any default value are set as null if the value is missing. It also adds `"default": null` to those fields when generating schema using avro4k.
     *
     * When set to `false`, during decoding, any missing value for a nullable field without default `null` value (e.g. `val field: Type?` without `= null`) is failing.
     */
    @ExperimentalSerializationApi
    val implicitNulls: Boolean = true,
    /**
     * By default, set to `true`, the array & map fields that haven't any default value are set as an empty array or map if the value is missing. It also adds `"default": []` for arrays or `"default": {}` for maps to those fields when generating schema using avro4k.
     *
     * If `implicitNulls` is true, the empty collections are set as null if the value is missing.
     *
     * When set to `false`, during decoding, any missing content for an array or a map field without its empty default value is failing.
     */
    @ExperimentalSerializationApi
    val implicitEmptyCollections: Boolean = true,
    /**
     * **To be removed when binary support is stable.**
     *
     * Set it to `true` to enable validation in case of failure, mainly for debug purpose.
     *
     * By default, to `false`.
     *
     * @see [org.apache.avro.io.ValidatingEncoder]
     * @see [org.apache.avro.io.ValidatingDecoder]
     */
    @ExperimentalSerializationApi
    val validateSerialization: Boolean = false,
)

/**
 * @see AvroConfiguration.fieldNamingStrategy
 */
public interface FieldNamingStrategy {
    public fun resolve(
        descriptor: SerialDescriptor,
        elementIndex: Int,
    ): String

    public companion object Builtins {
        /**
         * Simply returns the element name.
         */
        @ExperimentalSerializationApi
        public object OriginalElementName : FieldNamingStrategy {
            override fun resolve(
                descriptor: SerialDescriptor,
                elementIndex: Int,
            ): String = descriptor.getElementName(elementIndex)
        }

        /**
         * Convert the field name to snake_case by adding an underscore before each capital letter, and lowercase those capital letters.
         */
        public object SnakeCase : FieldNamingStrategy {
            override fun resolve(
                descriptor: SerialDescriptor,
                elementIndex: Int,
            ): String =
                descriptor.getElementName(elementIndex).let { serialName ->
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
        }

        /**
         * Enforce camelCase naming strategy by upper-casing the first field name letter.
         */
        @ExperimentalSerializationApi
        public object PascalCase : FieldNamingStrategy {
            override fun resolve(
                descriptor: SerialDescriptor,
                elementIndex: Int,
            ): String = descriptor.getElementName(elementIndex).replaceFirstChar { it.uppercaseChar() }
        }
    }
}