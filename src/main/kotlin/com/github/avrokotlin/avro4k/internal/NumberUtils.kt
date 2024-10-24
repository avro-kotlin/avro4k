package com.github.avrokotlin.avro4k.internal

import kotlinx.serialization.SerializationException
import java.math.BigDecimal

internal fun Int.toByteExact(): Byte {
    if (this.toByte().toInt() != this) {
        return invalidTypeError()
    }
    return this.toByte()
}

internal fun Long.toByteExact(): Byte {
    if (this.toByte().toLong() != this) {
        return invalidTypeError()
    }
    return this.toByte()
}

internal fun BigDecimal.toByteExact(): Byte {
    if (this.toByte().toInt().toBigDecimal() != this) {
        return invalidTypeError()
    }
    return this.toByte()
}

internal fun Int.toShortExact(): Short {
    if (this.toShort().toInt() != this) {
        return invalidTypeError()
    }
    return this.toShort()
}

internal fun Long.toShortExact(): Short {
    if (this.toShort().toLong() != this) {
        return invalidTypeError()
    }
    return this.toShort()
}

internal fun BigDecimal.toShortExact(): Short {
    if (this.toShort().toInt().toBigDecimal() != this) {
        return invalidTypeError()
    }
    return this.toShort()
}

internal fun Long.toIntExact(): Int {
    if (this.toInt().toLong() != this) {
        return invalidTypeError()
    }
    return this.toInt()
}

internal fun BigDecimal.toIntExact(): Int {
    if (this.toInt().toBigDecimal() != this) {
        return invalidTypeError()
    }
    return this.toInt()
}

internal fun BigDecimal.toLongExact(): Long {
    if (this.toLong().toBigDecimal() != this) {
        return invalidTypeError()
    }
    return this.toLong()
}

internal fun Double.toFloatExact(): Float {
    if (this.toFloat().toDouble() != this) {
        return invalidTypeError()
    }
    return this.toFloat()
}

internal fun BigDecimal.toDoubleExact(): Double {
    if (this.toDouble().toBigDecimal() != this) {
        return invalidTypeError()
    }
    return this.toDouble()
}

internal fun BigDecimal.toFloatExact(): Float {
    if (this.toFloat().toBigDecimal() != this) {
        return invalidTypeError()
    }
    return this.toFloat()
}

private inline fun <reified T> Any.invalidTypeError(): T {
    throw SerializationException("Value $this is not a valid ${T::class.simpleName}")
}