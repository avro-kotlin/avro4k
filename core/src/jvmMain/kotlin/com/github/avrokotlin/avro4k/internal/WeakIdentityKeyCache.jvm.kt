package com.github.avrokotlin.avro4k.internal

import com.github.avrokotlin.avro4k.InternalAvro4kApi
import java.lang.ref.ReferenceQueue
import java.lang.ref.WeakReference
import java.util.concurrent.ConcurrentHashMap

@InternalAvro4kApi
public actual class WeakIdentityKeyCache<K : Any, V> {
    private val queue = ReferenceQueue<K>()
    private val map = ConcurrentHashMap<Key<K>, V>()

    @InternalAvro4kApi
    public actual fun getOrPut(key: K, defaultValue: () -> V): V {
        removeStaleEntries()

        // "get" phase
        val hash = System.identityHashCode(key)
        map[Key.Lookup(hash, key)]?.let { return it }

        // "put" phase
        return map.computeIfAbsent(Key.Stored(hash, key, queue)) { defaultValue() }
    }

    private fun removeStaleEntries() {
        var ref = queue.poll() as Key.Stored<K>?
        while (ref != null) {
            map.remove(ref)
            ref = queue.poll() as Key.Stored<K>?
        }
    }

    private sealed interface Key<K : Any> {
        val value: K?
        class Stored<K : Any>(val hash: Int, value: K, queue: ReferenceQueue<K>) : Key<K>, WeakReference<K>(value, queue) {
            override fun hashCode(): Int = hash
            override val value: K? get() = get()

            override fun equals(other: Any?): Boolean {
                // The stale entries removed in removeStaleEntries() should be the same instance
                if (this === other) return true
                if (other !is Key<*>) return false

                val a = get()
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

                // Stale (null) entries are being removed, so they should not match
                val otherValue = other.value ?: return false

                return value === otherValue
            }
        }
    }
}