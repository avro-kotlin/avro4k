package com.github.avrokotlin.avro4k

import kotlinx.serialization.Serializable


@Serializable
enum class SomeEnum {
    A, B, C
}

@Serializable
@JvmInline
value class WrappedBoolean(val value: Boolean)

@Serializable
@JvmInline
value class WrappedByte(val value: Byte)

@Serializable
@JvmInline
value class WrappedChar(val value: Char)

@Serializable
@JvmInline
value class WrappedShort(val value: Short)

@Serializable
@JvmInline
value class WrappedInt(val value: Int)

@Serializable
@JvmInline
value class WrappedLong(val value: Long)

@Serializable
@JvmInline
value class WrappedFloat(val value: Float)

@Serializable
@JvmInline
value class WrappedDouble(val value: Double)

@Serializable
@JvmInline
value class WrappedString(val value: String)

@Serializable
@JvmInline
value class WrappedEnum(val value: SomeEnum)
