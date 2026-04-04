package com.github.avrokotlin.avro4k.internal

import com.github.avrokotlin.avro4k.InternalAvro4kApi

@InternalAvro4kApi
public expect class WeakIdentityKeyCache<K : Any, V>() {
    @InternalAvro4kApi
    public fun getOrPut(key: K, defaultValue: () -> V): V
}