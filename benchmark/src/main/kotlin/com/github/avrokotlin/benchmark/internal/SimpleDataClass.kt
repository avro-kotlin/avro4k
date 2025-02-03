package com.github.avrokotlin.benchmark.internal

import kotlinx.serialization.Serializable

@Serializable
internal data class SimpleDataClass(
    val bool: Boolean,
    val byte: Byte,
    val short: Short,
    val int: Int,
    val long: Long,
    val float: Float,
    val double: Double,
    val string: String,
    val bytes: ByteArray,
) {
    companion object {
        fun create(random: RandomUtils) = SimpleDataClass(
            bool = random.nextBoolean(),
            byte =  random.nextInt(Byte.MIN_VALUE.toInt(), Byte.MAX_VALUE.toInt()).toByte(),
            short = random.nextInt(Short.MIN_VALUE.toInt(), Short.MAX_VALUE.toInt()).toShort(),
            int = random.nextInt(),
            long = random.nextLong(),
            float = random.nextFloat(),
            double = random.nextDouble(),
            string = random.randomAlphanumeric(25),
            bytes = random.randomBytes(50),
        )
    }
}

@Serializable
internal data class SimpleDatasClass(
    val data: List<SimpleDataClass>
) {
    companion object {
        fun create(size: Int, random: RandomUtils) = SimpleDatasClass(
            data = List(size) { SimpleDataClass.create(random) }
        )
    }
}