package com.github.avrokotlin.benchmark.gen

import org.apache.commons.lang3.RandomStringUtils
import java.math.BigDecimal
import java.math.RoundingMode
import java.util.*
import kotlin.math.abs
import kotlin.random.Random
import kotlin.random.asJavaRandom

internal object RandomUtils {
    private val RANDOM: Random

    init {
        val seedStr = System.getenv("SEED")
        val seed =
            try {
                seedStr?.toLong()
            } catch (e: NumberFormatException) { null }?: System.nanoTime()
        println("Using SEED=$seed as seed for Random")
        RANDOM = Random(seed)
    }

    fun randomAlphabetic(count: Int): String {
        return random(count, true, false)
    }

    fun randomAlphanumeric(count: Int): String {
        return random(count, true, true)
    }

    fun randomNumeric(count: Int): String {
        return random(count, false, true)
    }

    fun random(
        count: Int,
        letters: Boolean,
        numbers: Boolean,
        start: Int = 0,
        end: Int = 0,
        chars: CharArray? = null
    ): String {
        return RandomStringUtils.random(count, start, end, letters, numbers, chars, RANDOM.asJavaRandom())
    }
    
    fun randomBytes(count: Int) : ByteArray  = RANDOM.nextBytes(count)

    fun randomBigDecimal(): BigDecimal {
        return BigDecimal.valueOf(RANDOM.nextDouble()).multiply(BigDecimal(1000)).setScale(4, RoundingMode.HALF_UP)
    }

    fun nextUUID(): UUID {
        return UUID(RANDOM.nextLong(), RANDOM.nextLong())
    }

    fun nextLong(): Long {
        return RANDOM.nextLong()
    }

    fun nextBoolean(): Boolean {
        return RANDOM.nextBoolean()
    }

    fun longArray(size: Int): LongArray {
        val arr = LongArray(size)
        for (i in 0..<size) {
            arr[i] = abs(RANDOM.nextInt().toDouble())
                .toLong()
        }
        return arr
    }

    fun stringArray(size: Int, count: Int): Array<String> {
        val arr = mutableListOf<String>()
        for (i in 0..<size) {
            arr.add(randomAlphabetic(count))
        }
        return arr.toTypedArray()
    }

    fun nextInt(bound: Int): Int {
        return RANDOM.nextInt(bound)
    }

    fun nextInt(startInclusive: Int, endExclusive: Int): Int {
        assert(endExclusive >= startInclusive)
        assert(startInclusive >= 0)
        return if (startInclusive == endExclusive) {
            startInclusive
        } else startInclusive + RANDOM.nextInt(
            endExclusive - startInclusive
        )
    }

    fun nextDouble(startInclusive: Double, endInclusive: Double): Double {
        assert(endInclusive >= startInclusive)
        assert(startInclusive >= 0)
        return if (startInclusive == endInclusive) {
            startInclusive
        } else startInclusive + (endInclusive - startInclusive) * RANDOM.nextDouble()
    }
}
