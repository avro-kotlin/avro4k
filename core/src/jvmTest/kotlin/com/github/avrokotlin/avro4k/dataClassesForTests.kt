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
internal data class RecordWithGenericField<T>(val field: T) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as RecordWithGenericField<*>

        return deepEquals(field, other.field)
    }

    override fun hashCode(): Int {
        return field?.hashCode() ?: 0
    }

    override fun toString(): String {
        if (field is ByteArray) {
            return "RecordWithGenericField(field=${field.contentToString()})"
        }
        return "RecordWithGenericField(field=$field)"
    }
}

@JvmInline
@Serializable
internal value class ValueClassWithGenericField<T>(val value: T) {
    override fun toString(): String {
        if (value is ByteArray) {
            return "ValueClassWithGenericField(value=${value.contentToString()})"
        }
        return "ValueClassWithGenericField(value=$value)"
    }
}

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