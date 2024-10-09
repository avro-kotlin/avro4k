package com.github.avrokotlin.avro4k.internal

import kotlinx.serialization.SerializationException
import java.math.BigDecimal

internal fun Int.toByteExact(): Byte {
    if (this.toByte().toInt() != this) {
        throw SerializationException("Value $this is not a valid Byte")
    }
    return this.toByte()
}

internal fun Long.toByteExact(): Byte {
    if (this.toByte().toLong() != this) {
        throw SerializationException("Value $this is not a valid Byte")
    }
    return this.toByte()
}

internal fun BigDecimal.toByteExact(): Byte {
    if (this.toInt().toByte().toInt().toBigDecimal() != this) {
        throw SerializationException("Value $this is not a valid Byte")
    }
    return this.toInt().toByte()
}

internal fun Int.toShortExact(): Short {
    if (this.toShort().toInt() != this) {
        throw SerializationException("Value $this is not a valid Short")
    }
    return this.toShort()
}

internal fun Long.toShortExact(): Short {
    if (this.toShort().toLong() != this) {
        throw SerializationException("Value $this is not a valid Short")
    }
    return this.toShort()
}

internal fun BigDecimal.toShortExact(): Short {
    if (this.toInt().toShort().toInt().toBigDecimal() != this) {
        throw SerializationException("Value $this is not a valid Short")
    }
    return this.toInt().toShort()
}

internal fun Long.toIntExact(): Int {
    if (this.toInt().toLong() != this) {
        throw invalidType<Int>()
    }
    return this.toInt()
}

internal fun BigDecimal.toIntExact(): Int {
    if (this.toInt().toBigDecimal() != this) {
        throw invalidType<Int>()
    }
    return this.toInt()
}

internal fun BigDecimal.toLongExact(): Long {
    if (this.toLong().toBigDecimal() != this) {
        throw invalidType<Long>()
    }
    return this.toLong()
}

internal fun Double.toFloatExact(): Float {
    if (this.toFloat().toDouble() != this) {
        throw invalidType<Float>()
    }
    return this.toFloat()
}

internal fun BigDecimal.toDoubleExact(): Double {
    if (this.toDouble().toBigDecimal() != this) {
        throw invalidType<Double>()
    }
    return this.toDouble()
}

internal fun BigDecimal.toFloatExact(): Float {
    if (this.toFloat().toBigDecimal() != this) {
        throw invalidType<Float>()
    }
    return this.toFloat()
}

private inline fun <reified T> Any.invalidType() = SerializationException("Value $this is not a valid ${T::class.simpleName}")