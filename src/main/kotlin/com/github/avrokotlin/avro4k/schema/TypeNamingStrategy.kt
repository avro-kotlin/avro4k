package com.github.avrokotlin.avro4k.schema

import kotlinx.serialization.descriptors.SerialDescriptor

interface TypeNamingStrategy {
    fun resolve(
        descriptor: SerialDescriptor,
        serialName: String,
    ): TypeName

    companion object Builtins {
        /**
         * Extract the record name from the fully qualified class name by taking the last part of the class name as the record name and the rest as the namespace.
         *
         * If there is no dot, then the namespace is null.
         */
        object FullyQualified : TypeNamingStrategy {
            override fun resolve(
                descriptor: SerialDescriptor,
                serialName: String,
            ): TypeName {
                val lastDot = serialName.lastIndexOf('.').takeIf { it >= 0 && it + 1 < serialName.length }
                val lastIndex = if (serialName.endsWith('?')) serialName.length - 1 else serialName.length
                return TypeName(
                    name = lastDot?.let { serialName.substring(lastDot + 1, lastIndex) } ?: serialName.substring(0, lastIndex),
                    namespace = lastDot?.let { serialName.substring(0, lastDot) }?.takeIf { it.isNotEmpty() }
                )
            }
        }
    }
}

data class TypeName(val name: String, val namespace: String?)