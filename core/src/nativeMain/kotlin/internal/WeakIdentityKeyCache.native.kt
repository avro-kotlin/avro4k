package com.github.avrokotlin.avro4k.internal

import com.github.avrokotlin.avro4k.InternalAvro4kApi
import kotlin.experimental.ExperimentalNativeApi
import kotlin.native.identityHashCode
import kotlin.native.ref.WeakReference

@InternalAvro4kApi
@OptIn(ExperimentalNativeApi::class)
public actual class WeakIdentityKeyCache<K : Any, V> {
    private val map = mutableMapOf<Key<K>, V>()

    internal val size: Int get() = map.size
    internal operator fun contains(key: K): Boolean = map.containsKey(Key.Lookup(key.identityHashCode(), key))

    @InternalAvro4kApi
    public actual fun getOrPut(key: K, defaultValue: () -> V): V {
        // "get" phase
        val hash = key.identityHashCode()
        map[Key.Lookup(hash, key)]?.let { return it }

        // Cleaning phase
        removeStaleEntries()

        // "put" phase
        return map.getOrPut(Key.Stored(hash, key)) { defaultValue() }
    }

    private fun removeStaleEntries() {
        map.keys.removeAll { (it as Key.Stored<*>).refence.value == null }
    }

    private sealed interface Key<K : Any> {
        val value: K?
        class Stored<K : Any>(val hash: Int, value: K) : Key<K> {
            val refence = WeakReference(value)

            override val value: K? get() = refence.value

            override fun hashCode(): Int = hash

            override fun equals(other: Any?): Boolean {
                if (this === other) return true
                if (other !is Key<*>) return false

                val a = value
                val b = other.value
                // Stale (null) entries are being removed, so they should not match
                if (a == null || b == null) return false

                return a === b
            }
        }

        /**
         * Strong reference to the key to ensure the key isn't garbage collected during lookup and to
         * prevent a WeakReference to be created and referenced for potential queuing just for lookup.
         */
        class Lookup<K : Any>(val hash: Int, override val value: K) : Key<K> {
            override fun hashCode(): Int = hash

            override fun equals(other: Any?): Boolean {
                if (this === other) return true
                if (other !is Key<*>) return false

                val a = value
                // Stale (null) entries are being removed, so they should not match
                val b = other.value ?: return false

                return a === b
            }
        }
    }
}