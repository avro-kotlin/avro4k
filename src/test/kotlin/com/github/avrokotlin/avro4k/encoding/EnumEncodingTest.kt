@file:UseSerializers(UUIDSerializer::class)

package com.github.avrokotlin.avro4k.encoding

import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.AvroEnumDefault
import com.github.avrokotlin.avro4k.record
import com.github.avrokotlin.avro4k.serializer.UUIDSerializer
import io.kotest.core.spec.style.StringSpec
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers

class EnumEncodingTest : StringSpec({

    "read / write enums" {
        AvroAssertions.assertThat(EnumTest(Cream.Bruce, BBM.Moore))
            .isEncodedAs(record("Bruce", "Moore"))
    }

    "read / write list of enums" {
        AvroAssertions.assertThat(EnumListTest(listOf(Cream.Bruce, Cream.Clapton)))
            .isEncodedAs(record(listOf("Bruce", "Clapton")))
    }

    "read / write nullable enums" {
        AvroAssertions.assertThat(NullableEnumTest(null))
            .isEncodedAs(record(null))
        AvroAssertions.assertThat(NullableEnumTest(Cream.Bruce))
            .isEncodedAs(record("Bruce"))
    }

    "Decoding enum with an unknown uses @AvroEnumDefault value" {
        AvroAssertions.assertThat(EnumV2WrapperRecord(EnumV2.B))
            .isEncodedAs(record("B"))
            .isDecodedAs(EnumV1WrapperRecord(EnumV1.UNKNOWN))
    }
}) {
    @Serializable
    @SerialName("EnumWrapper")
    data class EnumV1WrapperRecord(
        val value: EnumV1,
    )

    @Serializable
    @SerialName("EnumWrapper")
    data class EnumV2WrapperRecord(
        val value: EnumV2,
    )

    @Serializable
    @SerialName("Enum")
    @AvroEnumDefault("UNKNOWN")
    enum class EnumV1 {
        UNKNOWN,
        A,
    }

    @Serializable
    @SerialName("Enum")
    @AvroEnumDefault("UNKNOWN")
    enum class EnumV2 {
        UNKNOWN,
        A,
        B,
    }

    @Serializable
    data class EnumTest(val a: Cream, val b: BBM)

    @Serializable
    data class EnumListTest(val a: List<Cream>)

    @Serializable
    data class NullableEnumTest(val a: Cream?)

    enum class Cream {
        Bruce,
        Baker,
        Clapton,
    }

    enum class BBM {
        Bruce,
        Baker,
        Moore,
    }
}