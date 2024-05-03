@file:UseSerializers(UUIDSerializer::class)

package com.github.avrokotlin.avro4k.encoding

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.AvroEnumDefault
import com.github.avrokotlin.avro4k.record
import com.github.avrokotlin.avro4k.schema
import com.github.avrokotlin.avro4k.serializer.UUIDSerializer
import io.kotest.core.spec.style.StringSpec
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import org.apache.avro.generic.GenericData

class EnumEncodingTest : StringSpec({

    "read / write enums" {
        AvroAssertions.assertThat(EnumTest(Cream.Bruce, BBM.Moore))
            .isEncodedAs(record(GenericData.EnumSymbol(Avro.schema<Cream>(), "Bruce"), GenericData.EnumSymbol(Avro.schema<BBM>(), "Moore")))

        AvroAssertions.assertThat(Cream.Bruce)
            .isEncodedAs(GenericData.EnumSymbol(Avro.schema<Cream>(), "Bruce"))
        AvroAssertions.assertThat(CreamValueClass(Cream.Bruce))
            .isEncodedAs(GenericData.EnumSymbol(Avro.schema<Cream>(), "Bruce"))
    }

    "read / write list of enums" {
        AvroAssertions.assertThat(EnumListTest(listOf(Cream.Bruce, Cream.Clapton)))
            .isEncodedAs(record(listOf(GenericData.EnumSymbol(Avro.schema<Cream>(), "Bruce"), GenericData.EnumSymbol(Avro.schema<Cream>(), "Clapton"))))

        AvroAssertions.assertThat(listOf(Cream.Bruce, Cream.Clapton))
            .isEncodedAs(listOf(GenericData.EnumSymbol(Avro.schema<Cream>(), "Bruce"), GenericData.EnumSymbol(Avro.schema<Cream>(), "Clapton")))
        AvroAssertions.assertThat(listOf(CreamValueClass(Cream.Bruce), CreamValueClass(Cream.Clapton)))
            .isEncodedAs(listOf(GenericData.EnumSymbol(Avro.schema<Cream>(), "Bruce"), GenericData.EnumSymbol(Avro.schema<Cream>(), "Clapton")))
    }

    "read / write nullable enums" {
        AvroAssertions.assertThat(NullableEnumTest(null))
            .isEncodedAs(record(null))
        AvroAssertions.assertThat(NullableEnumTest(Cream.Bruce))
            .isEncodedAs(record(GenericData.EnumSymbol(Avro.schema<Cream>(), "Bruce")))

        AvroAssertions.assertThat<Cream?>(Cream.Bruce)
            .isEncodedAs(GenericData.EnumSymbol(Avro.schema<Cream>(), "Bruce"))
        AvroAssertions.assertThat<Cream?>(null)
            .isEncodedAs(null)

        AvroAssertions.assertThat<CreamValueClass?>(CreamValueClass(Cream.Bruce))
            .isEncodedAs(GenericData.EnumSymbol(Avro.schema<Cream>(), "Bruce"))
        AvroAssertions.assertThat<CreamValueClass?>(null)
            .isEncodedAs(null)
    }

    "Decoding enum with an unknown uses @AvroEnumDefault value" {
        AvroAssertions.assertThat(EnumV2WrapperRecord(EnumV2.B))
            .isEncodedAs(record(GenericData.EnumSymbol(Avro.schema<EnumV2>(), "B")))
            .isDecodedAs(EnumV1WrapperRecord(EnumV1.UNKNOWN))

        AvroAssertions.assertThat(EnumV2.B)
            .isEncodedAs(GenericData.EnumSymbol(Avro.schema<EnumV2>(), "B"))
            .isDecodedAs(EnumV1.UNKNOWN)
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
    @SerialName("Enum")
    @AvroEnumDefault("UNKNOWN")
    private enum class EnumV1 {
        UNKNOWN,
        A,
    }

    @Serializable
    @SerialName("Enum")
    @AvroEnumDefault("UNKNOWN")
    private enum class EnumV2 {
        UNKNOWN,
        A,
        B,
    }

    @Serializable
    private data class EnumTest(val a: Cream, val b: BBM)

    @JvmInline
    @Serializable
    private value class CreamValueClass(val a: Cream)

    @Serializable
    private data class EnumListTest(val a: List<Cream>)

    @Serializable
    private data class NullableEnumTest(val a: Cream?)

    private enum class Cream {
        Bruce,
        Baker,
        Clapton,
    }

    private enum class BBM {
        Bruce,
        Baker,
        Moore,
    }
}