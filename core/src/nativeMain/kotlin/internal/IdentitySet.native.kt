package com.github.avrokotlin.avro4k.internal

import kotlin.experimental.ExperimentalNativeApi
import kotlin.native.identityHashCode

internal actual class IdentitySet<T: Any> {
    private val backingSet: MutableSet<Item> = mutableSetOf()

    actual fun add(value: T): Boolean {
        return backingSet.add(Item(value))
    }

    actual fun remove(value: T): Boolean {
        return backingSet.remove(Item(value))
    }

    private inner class Item(val value: T) {
        @OptIn(ExperimentalNativeApi::class)
        override fun hashCode(): Int = value.identityHashCode()
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is IdentitySet<*>.Item) return false
            return value === other.value
        }
    }
}