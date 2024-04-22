package com.github.avrokotlin.avro4k.encoding

import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.record
import io.kotest.core.spec.style.StringSpec
import kotlinx.serialization.Serializable

class ArrayEncodingTest : StringSpec({
    "support array of booleans" {
        AvroAssertions.assertThat(TestArrayBooleans(listOf(true, false, true)))
            .isEncodedAs(record(listOf(true, false, true)))
    }
    "support List of doubles" {
        AvroAssertions.assertThat(TestListDoubles(listOf(12.54, 23.5, 9123.2314)))
            .isEncodedAs(record(listOf(12.54, 23.5, 9123.2314)))
    }
    "support List of records" {
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
    data class TestArrayBooleans(val booleans: List<Boolean>)

    @Serializable
    data class TestListDoubles(val doubles: List<Double>)

    @Serializable
    data class TestSetString(val strings: Set<String>)

    @Serializable
    data class TestArrayRecords(val records: Array<Record>)

    @Serializable
    data class TestListRecords(val records: List<Record>)

    @Serializable
    data class TestSetRecords(val records: Set<Record>)

    @Serializable
    data class Record(val str: String, val double: Double)
}