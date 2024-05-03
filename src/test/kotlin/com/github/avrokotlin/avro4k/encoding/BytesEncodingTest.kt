package com.github.avrokotlin.avro4k.encoding

import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.record
import io.kotest.core.spec.style.StringSpec
import kotlinx.serialization.Serializable
import org.apache.avro.Schema

internal class BytesEncodingTest : StringSpec({
    "encode/decode ByteArray" {
        AvroAssertions.assertThat(ByteArrayTest(byteArrayOf(1, 4, 9)))
            .isEncodedAs(record(byteArrayOf(1, 4, 9)))

        AvroAssertions.assertThat<ByteArray>()
            .generatesSchema(Schema.create(Schema.Type.BYTES))
        AvroAssertions.assertThat(byteArrayOf(1, 4, 9))
            .isEncodedAs(byteArrayOf(1, 4, 9))
    }

    "encode/decode List<Byte>" {
        AvroAssertions.assertThat(ListByteTest(listOf(1, 4, 9)))
            .isEncodedAs(record(byteArrayOf(1, 4, 9)))

        AvroAssertions.assertThat<List<Byte>>()
            .generatesSchema(Schema.create(Schema.Type.BYTES))
        AvroAssertions.assertThat(listOf<Byte>(1, 4, 9))
            .isEncodedAs(byteArrayOf(1, 4, 9))
    }

    "encode/decode Array<Byte> to ByteBuffer" {
        AvroAssertions.assertThat(ArrayByteTest(arrayOf(1, 4, 9)))
            .isEncodedAs(record(byteArrayOf(1, 4, 9)))

        AvroAssertions.assertThat<Array<Byte>>()
            .generatesSchema(Schema.create(Schema.Type.BYTES))
        AvroAssertions.assertThat(arrayOf<Byte>(1, 4, 9))
            .isEncodedAs(byteArrayOf(1, 4, 9))
    }
}) {
    @Serializable
    private data class ByteArrayTest(val z: ByteArray) {
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
    private data class ArrayByteTest(val z: Array<Byte>) {
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
    private data class ListByteTest(val z: List<Byte>)
}