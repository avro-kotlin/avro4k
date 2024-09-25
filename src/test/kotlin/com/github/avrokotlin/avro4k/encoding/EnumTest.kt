@file:UseSerializers(UUIDSerializer::class)

package com.github.avrokotlin.avro4k.encoding

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroAlias
import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.AvroDoc
import com.github.avrokotlin.avro4k.AvroEnumDefault
import com.github.avrokotlin.avro4k.RecordWithGenericField
import com.github.avrokotlin.avro4k.SomeEnum
import com.github.avrokotlin.avro4k.ValueClassWithGenericField
import com.github.avrokotlin.avro4k.basicScalarEncodeDecodeTests
import com.github.avrokotlin.avro4k.encodeToByteArray
import com.github.avrokotlin.avro4k.encodeToBytesUsingApacheLib
import com.github.avrokotlin.avro4k.record
import com.github.avrokotlin.avro4k.schema
import com.github.avrokotlin.avro4k.serializer.UUIDSerializer
import com.github.avrokotlin.avro4k.testSerializationTypeCompatibility
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerializationException
import kotlinx.serialization.UseSerializers
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData

internal class EnumTest : StringSpec({
    val expectedEnumSchema = SchemaBuilder.enumeration(Cream::class.qualifiedName).aliases("TheCream").doc("documentation").symbols("Bruce", "Baker", "Clapton")
    basicScalarEncodeDecodeTests<Cream>(Cream.Bruce, expectedEnumSchema, apacheCompatibleValue = GenericData.EnumSymbol(expectedEnumSchema, "Bruce"))
    testSerializationTypeCompatibility(Cream.Baker, "Baker")

    // TODO test alias decoding
    // TODO test decoding from union (name resolution)

    "Only allow 1 @AvroEnumDefault at max" {
        shouldThrow<UnsupportedOperationException> {
            Avro.schema<BadEnumWithManyDefaults>()
        }
        shouldThrow<UnsupportedOperationException> {
            Avro.schema<RecordWithGenericField<BadEnumWithManyDefaults>>()
        }
        shouldThrow<UnsupportedOperationException> {
            Avro.schema<ValueClassWithGenericField<BadEnumWithManyDefaults>>()
        }
    }

    "Decoding enum with an unknown symbol uses @AvroEnumDefault value" {
        Avro.schema<EnumV2>() shouldBe
            SchemaBuilder.enumeration("Enum")
                .defaultSymbol("UNKNOWN")
                .symbols("UNKNOWN", "A", "B")

        AvroAssertions.assertThat(EnumV2WrapperRecord(EnumV2.B))
            .isEncodedAs(record(GenericData.EnumSymbol(Avro.schema<EnumV2>(), "B")))
            .isDecodedAs(EnumV1WrapperRecord(EnumV1.UNKNOWN))

        AvroAssertions.assertThat(EnumV2.B)
            .isEncodedAs(GenericData.EnumSymbol(Avro.schema<EnumV2>(), "B"))
            .isDecodedAs(EnumV1.UNKNOWN)
    }

    "Decoding enum with an unknown symbol fails without @AvroEnumDefault, also ignoring default symbol in writer schema" {
        val schema = SchemaBuilder.enumeration("Enum").defaultSymbol("Z").symbols("X", "Z")

        val bytes = encodeToBytesUsingApacheLib(schema, GenericData.EnumSymbol(schema, "X"))
        shouldThrow<SerializationException> {
            Avro.decodeFromByteArray(schema, EnumV1WithoutDefault.serializer(), bytes)
        }
    }

    "Encoding enum with an unknown symbol fails even with default in writer schema" {
        val schema = SchemaBuilder.enumeration("Enum").defaultSymbol("Z").symbols("X", "Z")

        shouldThrow<SerializationException> {
            Avro.encodeToByteArray(schema, EnumV1WithoutDefault.A)
        }
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
    @SerialName("EnumWrapper")
    private data class EnumV1WrapperRecord(
        val value: EnumV1,
    )

    @Serializable
    @SerialName("EnumWrapper")
    private data class EnumV2WrapperRecord(
        val value: EnumV2,
    )

    @Serializable
    @SerialName("EnumWrapperRecord")
    private data class EnumWrapperRecord(
        val value: SomeEnum,
    )

    @Serializable
    @SerialName("Enum")
    private enum class EnumV1 {
        @AvroEnumDefault
        UNKNOWN,
        A,
    }

    @Serializable
    private enum class BadEnumWithManyDefaults {
        @AvroEnumDefault
        DEF1,

        @AvroEnumDefault
        DEF2,
    }

    @Serializable
    @SerialName("Enum")
    private enum class EnumV1WithoutDefault {
        UNKNOWN,
        A,
    }

    @Serializable
    @SerialName("Enum")
    private enum class EnumV2 {
        @AvroEnumDefault
        UNKNOWN,
        A,
        B,
    }

    @Serializable
    @AvroAlias("TheCream")
    @AvroDoc("documentation")
    private enum class Cream {
        Bruce,
        Baker,
        Clapton,
    }
}