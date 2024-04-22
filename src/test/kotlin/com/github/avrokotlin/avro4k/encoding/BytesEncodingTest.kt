package com.github.avrokotlin.avro4k.encoding

import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.AvroFixed
import com.github.avrokotlin.avro4k.record
import io.kotest.core.spec.style.StringSpec
import kotlinx.serialization.Serializable

class BytesEncodingTest : StringSpec({
    "encode/decode ByteArray" {
        AvroAssertions.assertThat(ByteArrayTest(byteArrayOf(1, 4, 9)))
            .isEncodedAs(record(byteArrayOf(1, 4, 9)))
    }
    "encode/decode List<Byte>" {
        AvroAssertions.assertThat(ListByteTest(listOf(1, 4, 9)))
            .isEncodedAs(record(byteArrayOf(1, 4, 9)))
    }

    "encode/decode Array<Byte> to ByteBuffer" {
        AvroAssertions.assertThat(ArrayByteTest(arrayOf(1, 4, 9)))
            .isEncodedAs(record(byteArrayOf(1, 4, 9)))
    }

    "encode/decode ByteArray as FIXED when schema is Type.Fixed" {
        AvroAssertions.assertThat(ByteArrayFixedTest(byteArrayOf(1, 4, 9)))
            .isEncodedAs(
                record(byteArrayOf(0, 0, 0, 0, 0, 1, 4, 9)),
                ByteArrayFixedTest(byteArrayOf(0, 0, 0, 0, 0, 1, 4, 9))
            )
    }
}) {
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

    @Serializable
    data class ByteArrayFixedTest(
        @AvroFixed(8) val z: ByteArray,
    ) {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as ByteArrayFixedTest

            return z.contentEquals(other.z)
        }

        override fun hashCode(): Int {
            return z.contentHashCode()
        }
    }

    @Serializable
    data class ArrayByteTest(val z: Array<Byte>) {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as ArrayByteTest

            return z.contentEquals(other.z)
        }

        override fun hashCode(): Int {
            return z.contentHashCode()
        }
    }

    @Serializable
    data class ListByteTest(val z: List<Byte>)
}