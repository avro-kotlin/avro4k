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
import org.apache.avro.SchemaParseException
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
        shouldThrow<SchemaParseException> {
            Avro.schema<InvalidEnumDefault>()
        }
        shouldThrow<SchemaParseException> {
            Avro.schema<RecordWithGenericField<InvalidEnumDefault>>()
        }
    }
}) {
    @Serializable
    @AvroAlias("MySuit")
    @AvroEnumDefault("DIAMONDS")
    @AvroDoc("documentation")
    private enum class Suit {
        SPADES,
        HEARTS,
        DIAMONDS,
        CLUBS,
    }

    @Serializable
    @AvroEnumDefault("PINEAPPLE")
    private enum class InvalidEnumDefault { VEGGIE, MEAT, }
}