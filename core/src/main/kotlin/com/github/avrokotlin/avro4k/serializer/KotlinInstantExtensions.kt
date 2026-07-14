@file:OptIn(kotlin.time.ExperimentalTime::class)

package com.github.avrokotlin.avro4k.serializer

import kotlinx.serialization.SerializationException
import kotlin.time.Instant

internal const val NANOSECONDS_PER_MILLISECOND: Long = 1_000_000L
internal const val NANOSECONDS_PER_MICROSECOND: Long = 1_000L
internal const val MILLIS_PER_SECOND: Long = 1_000L
internal const val MICROS_PER_SECOND: Long = 1_000_000L
internal const val NANOSECONDS_PER_SECOND: Long = 1_000_000_000L

internal fun Instant.toEpochUnits(
    unitsPerSecond: Long,
    logicalType: String,
): Long {
    try {
        val nanosPerUnit = NANOSECONDS_PER_SECOND / unitsPerSecond
        if (epochSeconds < 0 && nanosecondsOfSecond > 0) {
            val units = Math.multiplyExact(epochSeconds + 1, unitsPerSecond)
            val adjustment = nanosecondsOfSecond / nanosPerUnit - unitsPerSecond
            return Math.addExact(units, adjustment)
        }

        val units = Math.multiplyExact(epochSeconds, unitsPerSecond)
        return Math.addExact(units, nanosecondsOfSecond / nanosPerUnit)
    } catch (e: ArithmeticException) {
        throw SerializationException("$this is out of range for Avro $logicalType", e)
    }
}

internal fun Instant.Companion.fromEpochMicros(micros: Long): Instant =
    fromEpochUnits(micros, MICROS_PER_SECOND, NANOSECONDS_PER_MICROSECOND)

internal fun Instant.Companion.fromEpochNanos(nanos: Long): Instant =
    fromEpochUnits(nanos, NANOSECONDS_PER_SECOND, 1L)

private fun Instant.Companion.fromEpochUnits(
    units: Long,
    unitsPerSecond: Long,
    nanosPerUnit: Long,
): Instant = fromEpochSeconds(
    Math.floorDiv(units, unitsPerSecond),
    Math.floorMod(units, unitsPerSecond) * nanosPerUnit
)