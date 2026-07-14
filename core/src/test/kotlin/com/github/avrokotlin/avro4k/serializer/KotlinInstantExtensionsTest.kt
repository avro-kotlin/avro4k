@file:OptIn(kotlin.time.ExperimentalTime::class)

package com.github.avrokotlin.avro4k.serializer

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import kotlin.time.Instant

internal class KotlinInstantExtensionsTest : StringSpec({
    listOf(
        MILLIS_PER_SECOND to NANOSECONDS_PER_MILLISECOND,
        MICROS_PER_SECOND to NANOSECONDS_PER_MICROSECOND,
        NANOSECONDS_PER_SECOND to 1L
    ).forEach { (unitsPerSecond, nanosPerUnit) ->
        "convert the minimum representable $unitsPerSecond-units timestamp without intermediate overflow" {
            val instant = Instant.fromEpochSeconds(
                Math.floorDiv(Long.MIN_VALUE, unitsPerSecond),
                Math.floorMod(Long.MIN_VALUE, unitsPerSecond) * nanosPerUnit
            )

            instant.toEpochUnits(unitsPerSecond, "test") shouldBe Long.MIN_VALUE
        }

        "convert the maximum representable $unitsPerSecond-units timestamp" {
            val instant = Instant.fromEpochSeconds(
                Math.floorDiv(Long.MAX_VALUE, unitsPerSecond),
                Math.floorMod(Long.MAX_VALUE, unitsPerSecond) * nanosPerUnit
            )

            instant.toEpochUnits(unitsPerSecond, "test") shouldBe Long.MAX_VALUE
        }
    }

    "floor a fractional pre-epoch instant when converting to milliseconds" {
        val instant = Instant.fromEpochSeconds(-1, 123_456_789)

        instant.toEpochUnits(MILLIS_PER_SECOND, "test") shouldBe -877L
    }

    "create instants from the full microsecond timestamp range" {
        listOf(Long.MIN_VALUE, -876_544L, Long.MAX_VALUE).forEach { micros ->
            Instant.fromEpochMicros(micros) shouldBe Instant.fromEpochSeconds(
                Math.floorDiv(micros, MICROS_PER_SECOND),
                Math.floorMod(micros, MICROS_PER_SECOND) * NANOSECONDS_PER_MICROSECOND
            )
        }
    }

    "create instants from the full nanosecond timestamp range" {
        listOf(Long.MIN_VALUE, -876_543_211L, Long.MAX_VALUE).forEach { nanos ->
            Instant.fromEpochNanos(nanos) shouldBe Instant.fromEpochSeconds(
                Math.floorDiv(nanos, NANOSECONDS_PER_SECOND),
                Math.floorMod(nanos, NANOSECONDS_PER_SECOND)
            )
        }
    }
})