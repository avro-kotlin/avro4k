package com.github.avrokotlin.avro4k.internal

import com.github.avrokotlin.avro4k.InternalAvro4kApi


@InternalAvro4kApi
public actual class WeakIdentityKeyCache<K : Any, V> {
    @OptIn(ExperimentalWasmJsInterop::class)
    private val map: dynamic = js("new WeakMap()")

    @InternalAvro4kApi
    public actual fun getOrPut(key: K, defaultValue: () -> V): V {
        return map.getOrInsertComputed(key, defaultValue)
    }
}