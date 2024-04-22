package com.github.avrokotlin.avro4k.encoding

import com.github.avrokotlin.avro4k.AvroAlias
import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.AvroDefault
import com.github.avrokotlin.avro4k.record
import io.kotest.core.spec.style.StringSpec
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

class AvroAliasEncodingTest : StringSpec({
    "support alias on field" {
        AvroAssertions.assertThat(EncodedField("hello"))
            .isEncodedAs(record("hello"))
            .isDecodedAs(DecodedFieldWithAlias(5, "hello"))
    }

    "support alias on record" {
        AvroAssertions.assertThat(EncodedRecord("hello"))
            .isEncodedAs(record("hello"))
            .isDecodedAs(DecodedRecordWithAlias("hello"))
    }
}) {
    @Serializable
    @SerialName("Record")
    data class EncodedField(
        val s: String,
    )

    @Serializable
    @SerialName("Record")
    data class DecodedFieldWithAlias(
        @AvroDefault("5")
        val newField: Int,
        @AvroAlias("s") val str: String,
    )

    @Serializable
    @SerialName("RecordA")
    data class EncodedRecord(
        val field: String,
    )

    @Serializable
    @SerialName("RecordB")
    @AvroAlias("RecordA")
    data class DecodedRecordWithAlias(
        val field: String,
    )
}