package com.github.avrokotlin.avro4k.encoding

import com.github.avrokotlin.avro4k.AvroAssertions
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
    }

    "read write out bytes" {
        AvroAssertions.assertThat(ByteTest(3))
            .isEncodedAs(record(3))
    }

    "read write out shorts" {
        AvroAssertions.assertThat(ShortTest(3))
            .isEncodedAs(record(3))
    }

    "read write out chars" {
        AvroAssertions.assertThat(CharTest('A'))
            .isEncodedAs(record('A'.code))
    }

    "read write out strings" {
        AvroAssertions.assertThat(StringTest("Hello world"))
            .isEncodedAs(record("Hello world"))
    }

    "read write out longs" {
        AvroAssertions.assertThat(LongTest(65653L))
            .isEncodedAs(record(65653L))
    }

    "read write out ints" {
        AvroAssertions.assertThat(IntTest(44))
            .isEncodedAs(record(44))
    }

    "read write out doubles" {
        AvroAssertions.assertThat(DoubleTest(3.235))
            .isEncodedAs(record(3.235))
    }

    "read write out floats" {
        AvroAssertions.assertThat(FloatTest(3.4F))
            .isEncodedAs(record(3.4F))
    }
    "read write out byte arrays" {
        AvroAssertions.assertThat(ByteArrayTest("ABC".toByteArray()))
            .isEncodedAs(record(ByteBuffer.wrap("ABC".toByteArray())))
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