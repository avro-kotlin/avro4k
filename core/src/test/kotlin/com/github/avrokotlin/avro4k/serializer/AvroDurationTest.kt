package com.github.avrokotlin.avro4k.serializer

import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe

@OptIn(ExperimentalAvro4kApi::class)
internal class AvroDurationTest : StringSpec({
    listOf(
        "PT0.5S" to 500u,
        "PT4.5S" to 4500u,
        "PT4.05S" to 4050u,
        "PT4.005S" to 4005u,
        "PT4,5S" to 4500u,
        "PT4.S" to 4000u,
        "PT4S" to 4000u,
        "PT1M0.123S" to 60123u
    ).forEach { (text, millis) ->
        "parse $text" {
            AvroDuration.parse(text).millis shouldBe millis
            java.time.Duration.parse(text).toMillis().toUInt() shouldBe millis
        }
    }

    listOf(
        AvroDuration(0u, 0u, 0u) to "PT0S",
        AvroDuration(0u, 0u, 500u) to "PT0.5S",
        AvroDuration(0u, 0u, 4500u) to "PT4.5S",
        AvroDuration(0u, 0u, 4050u) to "PT4.05S",
        AvroDuration(0u, 0u, 4005u) to "PT4.005S",
        AvroDuration(0u, 0u, 4000u) to "PT4S",
        AvroDuration(1u, 2u, 3u) to "P1M2DT0.003S"
    ).forEach { (duration, text) ->
        "toString $text" {
            duration.toString() shouldBe text
            AvroDuration.parse(text) shouldBe duration
        }
    }
})