package com.github.avrokotlin.avro4k.encoding

import com.github.avrokotlin.avro4k.AvroAlias
import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.SomeEnum
import com.github.avrokotlin.avro4k.record
import com.github.avrokotlin.avro4k.recordWithSchema
import io.kotest.core.spec.style.StringSpec
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData

class AvroAliasEncodingTest : StringSpec({
    "support alias on field" {
        AvroAssertions.assertThat(EncodedField("hello"))
            .isEncodedAs(record("hello"))
            .isDecodedAs(DecodedFieldWithAlias(3, "hello"))
    }

    "support alias on record" {
        AvroAssertions.assertThat(EncodedRecord("hello"))
            .isEncodedAs(record("hello"))
            .isDecodedAs(DecodedRecordWithAlias("hello"))
    }

    "support alias on record inside an union" {
        val writerSchema =
            Schema.createUnion(
                SchemaBuilder.enumeration("OtherEnum").symbols("OTHER"),
                SchemaBuilder.record("UnknownRecord").aliases("RecordA")
                    .fields().name("field").type().stringType().noDefault()
                    .endRecord()
            )
        AvroAssertions.assertThat(EncodedRecord("hello"))
            .isEncodedAs(recordWithSchema(writerSchema.types[1], "hello"), writerSchema = writerSchema)
            .isDecodedAs(DecodedRecordWithAlias("hello"))
    }

    "support alias on enum" {
        val writerSchema =
            SchemaBuilder.record("EnumWrapperRecord").fields()
                .name("value")
                .type(
                    SchemaBuilder.enumeration("UnknownEnum").aliases("com.github.avrokotlin.avro4k.SomeEnum").symbols("A", "B", "C")
                )
                .noDefault()
                .endRecord()
        AvroAssertions.assertThat(EnumWrapperRecord(SomeEnum.A))
            .isEncodedAs(record(GenericData.EnumSymbol(writerSchema.fields[0].schema(), "A")), writerSchema = writerSchema)
    }

    "support alias on enum inside an union" {
        val writerSchema =
            SchemaBuilder.record("EnumWrapperRecord").fields()
                .name("value")
                .type(
                    Schema.createUnion(
                        SchemaBuilder.enumeration("OtherEnum").symbols("OTHER"),
                        SchemaBuilder.record("UnknownRecord").aliases("RecordA")
                            .fields().name("field").type().stringType().noDefault()
                            .endRecord(),
                        SchemaBuilder.enumeration("UnknownEnum").aliases("com.github.avrokotlin.avro4k.SomeEnum").symbols("A", "B", "C")
                    )
                )
                .noDefault()
                .endRecord()
        AvroAssertions.assertThat(EnumWrapperRecord(SomeEnum.A))
            .isEncodedAs(record(GenericData.EnumSymbol(writerSchema.fields[0].schema().types[2], "A")), writerSchema = writerSchema)
    }
}) {
    @Serializable
    @SerialName("Record")
    private data class EncodedField(
        val s: String,
    )

    @Serializable
    @SerialName("Record")
    private data class DecodedFieldWithAlias(
        val newField: Int = 3,
        @AvroAlias("s") val str: String,
    )

    @Serializable
    @SerialName("RecordA")
    private data class EncodedRecord(
        val field: String,
    )

    @Serializable
    @AvroAlias("RecordA")
    private data class DecodedRecordWithAlias(
        val field: String,
    )

    @Serializable
    @SerialName("EnumWrapperRecord")
    private data class EnumWrapperRecord(
        val value: SomeEnum,
    )
}