package com.github.avrokotlin.avro4k

import io.kotest.matchers.maps.shouldContainExactly
import org.apache.avro.generic.IndexedRecord


fun IndexedRecord.toMap(): Map<String, Any?> {
    val map = mutableMapOf<String, Any?>()
    for (field in schema.fields) {
        val value = this[field.pos()]
        map[field.name()] = normalizeValue(value)
    }
    return map
}

private fun normalizeValue(value: Any?): Any? = when (value) {
    is IndexedRecord -> value.toMap()
    is CharSequence -> value.toString()
    is Map<*, *> -> value.mapKeys { it.key.toString() }.mapValues { normalizeValue(it.value) }
    else -> value
}

infix fun Any?.shouldBeContentOf(other: IndexedRecord) {
    check(this is IndexedRecord) { "actual must be type of IndexedRecord, but was ${this?.let { it::class }}" }
    this.toMap() shouldContainExactly other.toMap()
}
