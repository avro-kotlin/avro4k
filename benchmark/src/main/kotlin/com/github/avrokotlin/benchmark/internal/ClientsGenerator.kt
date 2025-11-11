package com.github.avrokotlin.benchmark.internal

import java.time.Instant
import java.time.LocalDate
import kotlin.math.absoluteValue

internal object ClientsGenerator {
    fun generate(size: Int, random: RandomUtils) = Clients(
        buildList {
            repeat(size) { index ->
                add(Client(
                    partner = when (random.nextInt(4)) {
                        0 -> GoodPartner(random.nextLong(), random.randomAlphabetic(30), Instant.ofEpochMilli(random.nextLong()))
                        1 -> BadPartner(random.nextLong(), random.randomAlphabetic(30), Instant.ofEpochMilli(random.nextLong().absoluteValue))
                        2 -> if (random.nextBoolean()) Stranger.KNOWN_STRANGER else Stranger.UNKNOWN_STRANGER
                        3 -> null
                        else -> throw IllegalStateException("Unexpected value")
                    },
                    id = random.nextLong().absoluteValue,
                    index = random.nextInt(0, Int.MAX_VALUE),
                    isActive = random.nextBoolean(),
                    balance = if (index % 2 == 0) null else random.randomBigDecimal(),
                    picture = if (index % 3 == 0) null else random.randomBytes(4048),
                    age = random.nextInt(0, 100),
                    eyeColor = if (index % 2 == 0) null else EyeColor.entries[random.nextInt(3)],
                    name = if (index % 2 == 0) null else random.randomAlphanumeric(20),
                    gender = if (index % 2 == 0) null else if (random.nextBoolean()) 'M' else 'F',
                    company = if (index % 2 == 0) null else random.randomAlphanumeric(20),
                    emails = random.stringArray(random.nextInt(5, 10), 10),
                    phones = random.longArray(random.nextInt(5, 10)),
                    address = if (index % 2 == 0) null else random.randomAlphanumeric(20),
                    about = random.randomAlphanumeric(20),
                    registered = if (index % 3 == 0) null else
                        LocalDate.of(
                            1900 + random.nextInt(110),
                            1 + random.nextInt(12),
                            1 + random.nextInt(28)
                        ),
                    latitude = random.nextDouble(0.0, 90.0),
                    longitude = random.nextFloat(0.0f, 180.0f),
                    tags = buildList {
                        repeat(random.nextInt(5, 25)) {
                            add(if (it % 2 == 0) null else random.randomAlphanumeric(10))
                        }
                    },
                    map = buildMap {
                        repeat(10) {
                            put(random.randomAlphanumeric(10), random.randomAlphanumeric(10))
                        }
                    }
                ))
            }
        }
    )
}
