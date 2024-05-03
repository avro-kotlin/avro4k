package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroAlias
import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.AvroDoc
import com.github.avrokotlin.avro4k.AvroEnumDefault
import com.github.avrokotlin.avro4k.RecordWithGenericField
import com.github.avrokotlin.avro4k.nullable
import com.github.avrokotlin.avro4k.schema
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.StringSpec
import kotlinx.serialization.Serializable
import kotlin.io.path.Path

class EnumSchemaTest : StringSpec({
    "should generate schema with alias, enum default and doc" {
        AvroAssertions.assertThat<Suit>()
            .generatesSchema(Path("/enum_with_default.json"))
        AvroAssertions.assertThat<RecordWithGenericField<Suit>>()
            .generatesSchema(Path("/enum_with_default_record.json"))
    }
    "should generate nullable schema" {
        AvroAssertions.assertThat<Suit?>()
            .generatesSchema(Path("/enum_with_default.json")) { it.nullable }
    }
    "fail with unknown values" {
        shouldThrow<UnsupportedOperationException> {
            Avro.schema<InvalidEnumDefault>()
        }
        shouldThrow<UnsupportedOperationException> {
            Avro.schema<RecordWithGenericField<InvalidEnumDefault>>()
        }
    }
}) {
    @Serializable
    @AvroAlias("MySuit")
    @AvroDoc("documentation")
    private enum class Suit {
        SPADES,
        HEARTS,

        @AvroEnumDefault
        DIAMONDS,
        CLUBS,
    }

    @Serializable
    private enum class InvalidEnumDefault {
        @AvroEnumDefault
        VEGGIE,

        @AvroEnumDefault
        MEAT,
    }
}