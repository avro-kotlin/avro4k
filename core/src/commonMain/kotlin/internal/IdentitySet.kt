package com.github.avrokotlin.avro4k.internal

internal expect class IdentitySet<T: Any>() {
    fun add(value: T): Boolean
    fun remove(value: T): Boolean
}
