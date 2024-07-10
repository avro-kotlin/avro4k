package com.github.avrokotlin.avro4k.encoding

import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.internal.nullable
import com.github.avrokotlin.avro4k.record
import io.kotest.core.spec.style.StringSpec
import kotlinx.serialization.Serializable
import org.apache.avro.Schema

internal class BytesEncodingTest : StringSpec({
    "encode/decode nullable ByteArray to BYTES" {
        AvroAssertions.assertThat(NullableByteArrayTest(byteArrayOf(1, 4, 9)))
            .isEncodedAs(record(byteArrayOf(1, 4, 9)))
        AvroAssertions.assertThat(NullableByteArrayTest(null))
            .isEncodedAs(record(null))

        AvroAssertions.assertThat<ByteArray?>()
            .generatesSchema(Schema.create(Schema.Type.BYTES).nullable)
        AvroAssertions.assertThat(byteArrayOf(1, 4, 9))
            .isEncodedAs(byteArrayOf(1, 4, 9))
        AvroAssertions.assertThat<ByteArray?>(null)
            .isEncodedAs(null)
    }

    "encode/decode ByteArray to BYTES" {
        AvroAssertions.assertThat(ByteArrayTest(byteArrayOf(1, 4, 9)))
            .isEncodedAs(record(byteArrayOf(1, 4, 9)))

        AvroAssertions.assertThat<ByteArray>()
            .generatesSchema(Schema.create(Schema.Type.BYTES))
        AvroAssertions.assertThat(byteArrayOf(1, 4, 9))
            .isEncodedAs(byteArrayOf(1, 4, 9))
    }

    "encode/decode List<Byte> to ARRAY[INT]" {
        AvroAssertions.assertThat(ListByteTest(listOf(1, 4, 9)))
            .isEncodedAs(record(listOf(1, 4, 9)))

        AvroAssertions.assertThat<List<Byte>>()
            .generatesSchema(Schema.createArray(Schema.create(Schema.Type.INT)))
        AvroAssertions.assertThat(listOf<Byte>(1, 4, 9))
            .isEncodedAs(listOf(1, 4, 9))
    }

    "encode/decode Array<Byte> to ARRAY[INT]" {
        AvroAssertions.assertThat(ArrayByteTest(arrayOf(1, 4, 9)))
            .isEncodedAs(record(listOf(1, 4, 9)))

        AvroAssertions.assertThat<Array<Byte>>()
            .generatesSchema(Schema.createArray(Schema.create(Schema.Type.INT)))
        AvroAssertions.assertThat(arrayOf<Byte>(1, 4, 9))
            .isEncodedAs(listOf(1, 4, 9))
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
    private data class NullableByteArrayTest(val z: ByteArray?) {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as NullableByteArrayTest

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