package com.github.avrokotlin.avro4k.internal

internal actual class IdentitySet<T : Any> {
    private val backingSet: dynamic = js("new Set()")

    actual fun add(value: T): Boolean {
        if (backingSet.has(value)) {
            return false
        }
        backingSet.add(value)
        return true
    }

    actual fun remove(value: T): Boolean {
        return backingSet.delete(value)
    }
}