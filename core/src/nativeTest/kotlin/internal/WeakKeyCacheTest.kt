package com.github.avrokotlin.avro4k.internal

import kotlin.native.runtime.GC
import kotlin.native.runtime.NativeRuntimeApi
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotSame
import kotlin.test.assertSame
import kotlin.test.assertTrue

@OptIn(NativeRuntimeApi::class)
class WeakKeyCacheTest {
    private data class Key(val id: Int)

    @Test
    fun `entry is returned from cache when key is still referenced`() {
        val cache = WeakIdentityKeyCache<Key, Any>()
        var computeCount = 0

        val key = Key(1)
        val firstValue = Any()
        assertSame(firstValue, cache.getOrPut(key) { ++computeCount; firstValue })
        // Same reference, so reusing the first value
        assertSame(firstValue, cache.getOrPut(key) { ++computeCount; Any() })

        GC.collect()

        // Same reference, even after GC
        assertSame(firstValue, cache.getOrPut(key) { ++computeCount; Any() })
        // Value getter only gets executed once
        assertEquals(1, computeCount)
        // Only one unique entry in the cache
        assertEquals(1, cache.size)
        assertTrue(key in cache)
    }

    @Test
    fun `stale entry is recalculated when getOrPut is called after GC on the same key`() {
        val cache = WeakIdentityKeyCache<Key, Any>()

        fun addWeakEntry(id: Int): Any {
            val key = Key(id)
            return cache.getOrPut(key) { Any() }
            // key goes out of scope here
        }

        val firstValue = addWeakEntry(1)

        // Should not be the same reference, as the key is equal but not identical instance
        assertNotSame(firstValue, addWeakEntry(1))

        GC.collect()

        // The cache still contains the entry, even if it is no longer accessible,
        // thus waiting for the next put to trigger the eviction
        assertEquals(2, cache.size)
        // Should be a new reference, as the key is no longer accessible
        assertNotSame(firstValue, addWeakEntry(1))
        // Stale entries should be cleaned after a put
        assertEquals(1, cache.size)
    }

    @Test
    fun `stale entries are not cleaned when getOrPut hits the cache`() {
        val cache = WeakIdentityKeyCache<Key, Any>()

        fun addWeakEntry(id: Int): Any {
            val key = Key(id)
            return cache.getOrPut(key) { Any() }
            // key goes out of scope here
        }

        val liveKey = Key(0)
        val valueFromLiveKey = cache.getOrPut(liveKey) { Any() }

        val value1FromWeakKey = addWeakEntry(1)
        val value2FromWeakKey = addWeakEntry(2)

        GC.collect()
        // The cache still contains the stale entries, even if it is no longer accessible,
        // thus waiting for the next put to trigger the eviction
        assertEquals(3, cache.size)
        assertTrue(liveKey in cache)

        // Cache hit on liveKey → no cleanup triggered, stale entries remain
        assertSame(valueFromLiveKey, cache.getOrPut(liveKey) { Any() })
        assertEquals(3, cache.size)

        // Now, any unknown key will first remove the stale entries, then store the new value
        addWeakEntry(1000)
        // Size now 2 as the Key(1) and Key(2) has been evicted while Key(0) remains and Key(1000) added
        assertEquals(2, cache.size)

        // As stale entries have been cleaned up during the Key(1000) miss
        assertNotSame(value1FromWeakKey, addWeakEntry(1))
        assertNotSame(value2FromWeakKey, addWeakEntry(2))
    }
}
