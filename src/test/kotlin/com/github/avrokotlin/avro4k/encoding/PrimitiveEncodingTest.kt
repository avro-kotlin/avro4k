package com.github.avrokotlin.avro4k.encoding

import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.WrappedBoolean
import com.github.avrokotlin.avro4k.WrappedByte
import com.github.avrokotlin.avro4k.WrappedChar
import com.github.avrokotlin.avro4k.WrappedDouble
import com.github.avrokotlin.avro4k.WrappedFloat
import com.github.avrokotlin.avro4k.WrappedInt
import com.github.avrokotlin.avro4k.WrappedLong
import com.github.avrokotlin.avro4k.WrappedShort
import com.github.avrokotlin.avro4k.WrappedString
import com.github.avrokotlin.avro4k.record
import io.kotest.core.spec.style.StringSpec
import kotlinx.serialization.Serializable
import java.nio.ByteBuffer

class PrimitiveEncodingTest : StringSpec({
    "read write out booleans" {
        AvroAssertions.assertThat(BooleanTest(true))
            .isEncodedAs(record(true))
        AvroAssertions.assertThat(BooleanTest(false))
            .isEncodedAs(record(false))
        AvroAssertions.assertThat(true)
            .isEncodedAs(true)
        AvroAssertions.assertThat(false)
            .isEncodedAs(false)
        AvroAssertions.assertThat(WrappedBoolean(true))
            .isEncodedAs(true)
        AvroAssertions.assertThat(WrappedBoolean(false))
            .isEncodedAs(false)
    }

    "read write out bytes" {
        AvroAssertions.assertThat(ByteTest(3))
            .isEncodedAs(record(3))
        AvroAssertions.assertThat(3.toByte())
            .isEncodedAs(3)
        AvroAssertions.assertThat(WrappedByte(3))
            .isEncodedAs(3)
    }

    "read write out shorts" {
        AvroAssertions.assertThat(ShortTest(3))
            .isEncodedAs(record(3))
        AvroAssertions.assertThat(3.toShort())
            .isEncodedAs(3)
        AvroAssertions.assertThat(WrappedShort(3))
            .isEncodedAs(3)
    }

    "read write out chars" {
        AvroAssertions.assertThat(CharTest('A'))
            .isEncodedAs(record('A'.code))
        AvroAssertions.assertThat('A')
            .isEncodedAs('A'.code)
        AvroAssertions.assertThat(WrappedChar('A'))
            .isEncodedAs('A'.code)
    }

    "read write out strings" {
        AvroAssertions.assertThat(StringTest("Hello world"))
            .isEncodedAs(record("Hello world"))
        AvroAssertions.assertThat("Hello world")
            .isEncodedAs("Hello world")
        AvroAssertions.assertThat(WrappedString("Hello world"))
            .isEncodedAs("Hello world")
    }

    "read write out longs" {
        AvroAssertions.assertThat(LongTest(65653L))
            .isEncodedAs(record(65653L))
        AvroAssertions.assertThat(65653L)
            .isEncodedAs(65653L)
        AvroAssertions.assertThat(WrappedLong(65653))
            .isEncodedAs(65653L)
    }

    "read write out ints" {
        AvroAssertions.assertThat(IntTest(44))
            .isEncodedAs(record(44))
        AvroAssertions.assertThat(44)
            .isEncodedAs(44)
        AvroAssertions.assertThat(WrappedInt(44))
            .isEncodedAs(44)
    }

    "read write out doubles" {
        AvroAssertions.assertThat(DoubleTest(3.235))
            .isEncodedAs(record(3.235))
        AvroAssertions.assertThat(3.235)
            .isEncodedAs(3.235)
        AvroAssertions.assertThat(WrappedDouble(3.235))
            .isEncodedAs(3.235)
    }

    "read write out floats" {
        AvroAssertions.assertThat(FloatTest(3.4F))
            .isEncodedAs(record(3.4F))
        AvroAssertions.assertThat(3.4F)
            .isEncodedAs(3.4F)
        AvroAssertions.assertThat(WrappedFloat(3.4F))
            .isEncodedAs(3.4F)
    }

    "read write out byte arrays" {
        AvroAssertions.assertThat(ByteArrayTest("ABC".toByteArray()))
            .isEncodedAs(record(ByteBuffer.wrap("ABC".toByteArray())))
        AvroAssertions.assertThat("ABC".toByteArray())
            .isEncodedAs(ByteBuffer.wrap("ABC".toByteArray()))
    }
}) {
    @Serializable
    data class BooleanTest(val z: Boolean)

    @Serializable
    data class ByteTest(val z: Byte)

    @Serializable
    data class ShortTest(val z: Short)

    @Serializable
    data class CharTest(val z: Char)

    @Serializable
    data class StringTest(val z: String)

    @Serializable
    data class FloatTest(val z: Float)

    @Serializable
    data class DoubleTest(val z: Double)

    @Serializable
    data class IntTest(val z: Int)

    @Serializable
    data class LongTest(val z: Long)

    @Serializable
    data class ByteArrayTest(val z: ByteArray) {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as ByteArrayTest

            return z.contentEquals(other.z)
        }

        override fun hashCode(): Int {
            return z.contentHashCode()
        }
    }
}