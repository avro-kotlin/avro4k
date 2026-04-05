package com.github.avrokotlin.avro4k.internal

import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class IdentitySetTest {
    @Test
    fun `adding an item twice returns false`() {
        val set = IdentitySet<String>()

        assertTrue(set.add("foo"))
        assertTrue(set.add("bar"))

        assertFalse(set.add("foo"))
    }

    @Test
    fun `adding equals items twice but in 2 different instances returns true`() {
        data class Item(val value: String)
        val set = IdentitySet<Item>()
        assertTrue(set.add(Item("foo")))
        assertTrue(set.add(Item("foo")))
    }

    @Test
    fun `removing an item twice returns true then false for the same item`() {
        val set = IdentitySet<String>()
        set.add("foo")
        set.add("bar")

        assertTrue(set.remove("foo"))
        assertFalse(set.remove("foo"))
    }

    @Test
    fun `removing from empty set returns false`() {
        val set = IdentitySet<String>()
        assertFalse(set.remove("foo"))
    }

    @Test
    fun `removing an item that was never added returns false`() {
        val set = IdentitySet<String>()
        set.add("bar")
        assertFalse(set.remove("foo"))
    }

    @Test
    fun `removing equal but different instance does not remove the original`() {
        data class Item(val value: String)
        val set = IdentitySet<Item>()
        val item = Item("foo")
        set.add(item)

        assertFalse(set.remove(Item("foo")))
        assertTrue(set.remove(item))
    }

    @Test
    fun `re-adding after removal returns true`() {
        val set = IdentitySet<String>()
        val item = "foo"

        assertTrue(set.add(item))
        assertTrue(set.remove(item))
        assertTrue(set.add(item))
    }

    @Test
    fun `adding multiple distinct instances works independently`() {
        val set = IdentitySet<String>()
        val a = "alpha"
        val b = "beta"
        val c = "gamma"

        assertTrue(set.add(a))
        assertTrue(set.add(b))
        assertTrue(set.add(c))

        assertFalse(set.add(a))
        assertFalse(set.add(b))
        assertFalse(set.add(c))

        assertTrue(set.remove(b))
        assertFalse(set.remove(b))
        assertFalse(set.add(a))
        assertTrue(set.add(b))
    }
}