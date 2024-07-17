package com.github.avrokotlin.avro4k.encoding

import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.record
import io.kotest.core.spec.style.StringSpec
import kotlinx.serialization.Serializable

internal class ArrayEncodingTest : StringSpec({
    "support array of booleans" {
        @Serializable
        data class TestArrayBooleans(val booleans: List<Boolean>)

        AvroAssertions.assertThat(TestArrayBooleans(listOf(true, false, true)))
            .isEncodedAs(record(listOf(true, false, true)))
    }
    "support array of nullable booleans" {
        @Serializable
        data class TestArrayBooleans(val booleans: List<Boolean?>)

        AvroAssertions.assertThat(TestArrayBooleans(listOf(true, null, false)))
            .isEncodedAs(record(listOf(true, null, false)))
    }
    "support List of doubles" {
        AvroAssertions.assertThat(TestListDoubles(listOf(12.54, 23.5, 9123.2314)))
            .isEncodedAs(record(listOf(12.54, 23.5, 9123.2314)))
    }
    "support List of records" {
        @Serializable
        data class TestListRecords(val records: List<Record>)

        AvroAssertions.assertThat(
            TestListRecords(
                listOf(
                    Record("qwe", 123.4),
                    Record("wer", 8234.324)
                )
            )
        ).isEncodedAs(
            record(
                listOf(
                    record("qwe", 123.4),
                    record("wer", 8234.324)
                )
            )
        )
    }
    "support List of nullable records" {
        @Serializable
        data class TestListNullableRecords(val records: List<Record?>)

        AvroAssertions.assertThat(
            TestListNullableRecords(
                listOf(
                    Record("qwe", 123.4),
                    null,
                    Record("wer", 8234.324)
                )
            )
        ).isEncodedAs(
            record(
                listOf(
                    record("qwe", 123.4),
                    null,
                    record("wer", 8234.324)
                )
            )
        )
    }
    "support Set of records" {
        AvroAssertions.assertThat(
            TestSetRecords(
                setOf(
                    Record("qwe", 123.4),
                    Record("wer", 8234.324)
                )
            )
        ).isEncodedAs(
            record(
                listOf(
                    record("qwe", 123.4),
                    record("wer", 8234.324)
                )
            )
        )
    }

    "support Set of strings" {
        AvroAssertions.assertThat(TestSetString(setOf("Qwe", "324", "q")))
            .isEncodedAs(record(listOf("Qwe", "324", "q")))
    }
}) {
    @Serializable
    private data class TestListDoubles(val doubles: List<Double>)

    @Serializable
    private data class TestSetString(val strings: Set<String>)

    @Serializable
    private data class TestSetRecords(val records: Set<Record>)

    @Serializable
    private data class Record(val str: String, val double: Double)
}