package com.github.avrokotlin.benchmark.internal

import java.time.Instant
import java.time.LocalDate
import kotlin.math.absoluteValue

internal object ClientsGenerator {
    fun generate(size: Int) = Clients(
        clients = buildList {
            repeat(size) {
                add(newClient())
            }
        }
    )

    private fun newClient(): Client {
        val u = Client(
            partner = when (RandomUtils.nextInt(3)) {
                0 -> GoodPartner(RandomUtils.nextLong(), RandomUtils.randomAlphabetic(30), Instant.ofEpochMilli(RandomUtils.nextLong()))
                1 -> BadPartner(RandomUtils.nextLong(), RandomUtils.randomAlphabetic(30), Instant.ofEpochMilli(RandomUtils.nextLong().absoluteValue))
                2 -> if (RandomUtils.nextBoolean()) Stranger.KNOWN_STRANGER else Stranger.UNKNOWN_STRANGER
                else -> throw IllegalStateException("Unexpected value")
            },
            id = RandomUtils.nextLong().absoluteValue,
            index = RandomUtils.nextInt(0, Int.MAX_VALUE),
            isActive = RandomUtils.nextBoolean(),
            balance = RandomUtils.randomBigDecimal(),
            picture = RandomUtils.randomBytes(4048),
            age = RandomUtils.nextInt(0, 100),
            eyeColor = EyeColor.entries[RandomUtils.nextInt(3)],
            name = RandomUtils.randomAlphanumeric(20),
            gender = if (RandomUtils.nextBoolean()) 'M' else 'F',
            company = RandomUtils.randomAlphanumeric(20),
            emails = RandomUtils.stringArray(RandomUtils.nextInt(5, 10), 10),
            phones = RandomUtils.longArray(RandomUtils.nextInt(5, 10)),
            address = RandomUtils.randomAlphanumeric(20),
            about = RandomUtils.randomAlphanumeric(20),
            registered =
            LocalDate.of(
                1900 + RandomUtils.nextInt(110),
                1 + RandomUtils.nextInt(12),
                1 + RandomUtils.nextInt(28)
            ),
            latitude = RandomUtils.nextDouble(0.0, 90.0),
            longitude = RandomUtils.nextFloat(0.0f, 180.0f),
            tags = buildList {
                repeat(RandomUtils.nextInt(5, 25)) {
                    add(RandomUtils.randomAlphanumeric(10))
                }
            },
            map = buildMap {
                repeat(10) {
                    put(RandomUtils.randomAlphanumeric(10), RandomUtils.randomAlphanumeric(10))
                }
            }
        )
        return u
    }
}

