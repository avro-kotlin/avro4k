@file:UseSerializers(UUIDSerializer::class)

package com.github.avrokotlin.avro4k.encoding

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.AvroEnumDefault
import com.github.avrokotlin.avro4k.basicScalarEncodeDecodeTests
import com.github.avrokotlin.avro4k.encodeToByteArray
import com.github.avrokotlin.avro4k.encodeToBytesUsingApacheLib
import com.github.avrokotlin.avro4k.record
import com.github.avrokotlin.avro4k.schema
import com.github.avrokotlin.avro4k.serializer.UUIDSerializer
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.StringSpec
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerializationException
import kotlinx.serialization.UseSerializers
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData

internal class EnumEncodingTest : StringSpec({

    basicScalarEncodeDecodeTests(Cream.Bruce, Avro.schema<Cream>(), apacheCompatibleValue = GenericData.EnumSymbol(Avro.schema<Cream>(), "Bruce"))

    "Decoding enum with an unknown symbol uses @AvroEnumDefault value" {
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
    private enum class EnumV1 {
        @AvroEnumDefault
        UNKNOWN,
        A,
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
    private enum class Cream {
        Bruce,
        Baker,
        Clapton,
    }
}