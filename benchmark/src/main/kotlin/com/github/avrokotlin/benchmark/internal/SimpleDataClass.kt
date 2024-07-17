package com.github.avrokotlin.benchmark.internal

import kotlinx.serialization.Serializable

@Serializable
data class SimpleDataClass(
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
        fun create() = SimpleDataClass(
            bool = RandomUtils.nextBoolean(),
            byte =  RandomUtils.nextInt(Byte.MIN_VALUE.toInt(), Byte.MAX_VALUE.toInt()).toByte(),
            short = RandomUtils.nextInt(Short.MIN_VALUE.toInt(), Short.MAX_VALUE.toInt()).toShort(),
            int = RandomUtils.nextInt(),
            long = RandomUtils.nextLong(),
            float = RandomUtils.nextFloat(),
            double = RandomUtils.nextDouble(),
            string = RandomUtils.randomAlphanumeric(25),
            bytes = RandomUtils.randomBytes(50),
        )
    }
}

@Serializable
data class SimpleDatasClass(
    val data: List<SimpleDataClass>
) {
    companion object {
        fun create(size: Int) = SimpleDatasClass(
            data = List(size) { SimpleDataClass.create() }
        )
    }
}