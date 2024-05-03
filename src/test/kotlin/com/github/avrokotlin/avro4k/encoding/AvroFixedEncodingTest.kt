package com.github.avrokotlin.avro4k.encoding

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.AvroFixed
import com.github.avrokotlin.avro4k.encodeToByteArray
import com.github.avrokotlin.avro4k.record
import com.github.avrokotlin.avro4k.schema
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.StringSpec
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerializationException
import org.apache.avro.generic.GenericData
import kotlin.io.path.Path

internal class AvroFixedEncodingTest : StringSpec({
    "support fixed on data class fields" {
        AvroAssertions.assertThat<FixedStringField>()
            .generatesSchema(Path("/fixed_string.json"))

        val schema = Avro.schema<FixedStringField>().fields[0].schema()
        AvroAssertions.assertThat(FixedStringField("1234567"))
            .isEncodedAs(record(GenericData.Fixed(schema, "1234567".toByteArray())))
    }

    "support fixed on value classes" {
        AvroAssertions.assertThat<FixedNestedStringField>()
            .generatesSchema(Path("/fixed_string.json"))

        val schema = Avro.schema<FixedNestedStringField>().fields[0].schema()
        AvroAssertions.assertThat(FixedNestedStringField(FixedStringValueClass("1234567")))
            .isEncodedAs(record(GenericData.Fixed(schema, "1234567".toByteArray())))

        AvroAssertions.assertThat(FixedStringValueClass("1234567"))
            .isEncodedAs(GenericData.Fixed(Avro.schema<FixedStringValueClass>(), "1234567".toByteArray()))
    }

    "top-est @AvroFixed annotation takes precedence over nested @AvroFixed annotations" {
        AvroAssertions.assertThat<FieldPriorToValueClass>()
            .generatesSchema(Path("/fixed_string_5.json"))

        // Not 5 chars fixed
        shouldThrow<SerializationException> {
            Avro.encodeToByteArray(FieldPriorToValueClass(FixedStringValueClass("1234567")))
        }

        val schema = Avro.schema<FieldPriorToValueClass>().fields[0].schema()
        AvroAssertions.assertThat(FieldPriorToValueClass(FixedStringValueClass("12345")))
            .isEncodedAs(record(GenericData.Fixed(schema, "12345".toByteArray())))
    }

    "encode/decode ByteArray as FIXED when schema is Type.Fixed" {
        AvroAssertions.assertThat(ByteArrayFixedTest(byteArrayOf(1, 4, 9)))
            .isEncodedAs(
                record(byteArrayOf(0, 0, 0, 0, 0, 1, 4, 9)),
                expectedDecodedValue = ByteArrayFixedTest(byteArrayOf(0, 0, 0, 0, 0, 1, 4, 9))
            )
    }

//    "Handle FIXED in unions with the good and bad fullNames and aliases" {
//        fail("TODO")
//    }
}) {
    @Serializable
    @SerialName("Fixed")
    private data class FixedStringField(
        @AvroFixed(7) val mystring: String,
    )

    @Serializable
    @SerialName("Fixed")
    private data class FixedNestedStringField(
        val mystring: FixedStringValueClass,
    )

    @Serializable
    @SerialName("Fixed")
    private data class FieldPriorToValueClass(
        @AvroFixed(5) val mystring: FixedStringValueClass,
    )

    @JvmInline
    @Serializable
    @SerialName("FixedString")
    private value class FixedStringValueClass(
        @AvroFixed(7) val mystring: String,
    )

    @Serializable
    private data class ByteArrayFixedTest(
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
}