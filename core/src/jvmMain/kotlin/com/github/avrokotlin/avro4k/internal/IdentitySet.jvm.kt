package com.github.avrokotlin.avro4k.internal

import java.util.Collections
import java.util.IdentityHashMap

internal actual class IdentitySet<T : Any> {
    private val backingSet: MutableSet<T> = Collections.newSetFromMap(IdentityHashMap<T, Boolean>())

    actual fun add(value: T): Boolean {
        return backingSet.add(value)
    }

    actual fun remove(value: T): Boolean {
        return backingSet.remove(value)
    }
}