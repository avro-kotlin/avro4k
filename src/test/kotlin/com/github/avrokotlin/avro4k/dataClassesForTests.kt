package com.github.avrokotlin.avro4k

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
internal enum class SomeEnum {
    A,
    B,
    C,
}

@Serializable
@SerialName("RecordWithGenericField")
internal data class RecordWithGenericField<T>(val field: T)

@JvmInline
@Serializable
internal value class ValueClassWithGenericField<T>(val value: T)

@Serializable
@JvmInline
internal value class WrappedBoolean(val value: Boolean)

@Serializable
@JvmInline
internal value class WrappedByte(val value: Byte)

@Serializable
@JvmInline
internal value class WrappedChar(val value: Char)

@Serializable
@JvmInline
internal value class WrappedShort(val value: Short)

@Serializable
@JvmInline
internal value class WrappedInt(val value: Int)

@Serializable
@JvmInline
internal value class WrappedLong(val value: Long)

@Serializable
@JvmInline
internal value class WrappedFloat(val value: Float)

@Serializable
@JvmInline
internal value class WrappedDouble(val value: Double)

@Serializable
@JvmInline
internal value class WrappedString(val value: String)

@Serializable
@JvmInline
internal value class WrappedEnum(val value: SomeEnum)