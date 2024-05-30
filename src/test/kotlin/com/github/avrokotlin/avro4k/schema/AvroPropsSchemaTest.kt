package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.AvroEnumDefault
import com.github.avrokotlin.avro4k.AvroProp
import com.github.avrokotlin.avro4k.schema
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.StringSpec
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.SchemaParseException
import kotlin.io.path.Path

internal class AvroPropsSchemaTest : StringSpec({
    "should support props annotation on class" {
        AvroAssertions.assertThat<TypeAnnotated>()
            .generatesSchema(Path("/props_json_annotation_class.json"))
    }
    "should add props on the contained type of a value class" {
        val stringSchema =
            Schema.create(Schema.Type.STRING).also {
                it.addProp("cold", "play")
            }

        AvroAssertions.assertThat<CustomPropOnClass>()
            .generatesSchema(stringSchema)
        AvroAssertions.assertThat<CustomPropOnField>()
            .generatesSchema(stringSchema)
        AvroAssertions.assertThat<CustomPropOnFieldPriorToClass>()
            .generatesSchema(stringSchema)
    }
    "props in value class is only applying to underlying type but not the enclosing record field" {
        val stringSchema =
            Schema.create(Schema.Type.STRING).also {
                it.addProp("cold", "play")
            }

        @SerialName("SimpleDataClass")
        @Serializable
        data class SimpleDataClass(
            val customPropOnClass: CustomPropOnClass,
            val customPropOnField: CustomPropOnField,
        )
        AvroAssertions.assertThat<SimpleDataClass>()
            .generatesSchema(
                SchemaBuilder.record("SimpleDataClass").fields()
                    .name("customPropOnClass").type(stringSchema).noDefault()
                    .name("customPropOnField").type(stringSchema).noDefault()
                    .endRecord()
            )
    }
    "when props are added to a record using props in value class and also reusing the record but without the same props fails" {
        @Serializable
        data class SimpleDataClass(
            val basicRecord: BasicRecord,
            val basicRecordWithProps: BasicRecordWithProps,
        )
        shouldThrow<SchemaParseException> {
            //
            Avro.schema<SimpleDataClass>().toString()
        }
    }
}) {
    @Serializable
    @SerialName("BasicRecord")
    private data class BasicRecord(
        val field: String,
    )

    @JvmInline
    @Serializable
    private value class BasicRecordWithProps(
        @AvroProp("cold", "play")
        val value: BasicRecord,
    )

    @JvmInline
    @Serializable
    @AvroProp("cold", "play")
    private value class CustomPropOnClass(
        val value: String,
    )

    @JvmInline
    @Serializable
    @AvroProp("cold", "ignored")
    private value class CustomPropOnFieldPriorToClass(
        @AvroProp("cold", "play")
        val value: String,
    )

    @JvmInline
    @Serializable
    private value class CustomPropOnField(
        @AvroProp("cold", "play")
        val value: String,
    )

    @Serializable
    @AvroProp("hey", "there")
    @AvroProp("counting", """["one", "two"]""")
    private data class TypeAnnotated(
        @AvroProp("cold", "play")
        @AvroProp(
            key = "complexObject",
            value = """{
           "a": "foo",
           "b": 200,
           "c": true,
           "d": null,
           "e": { "e1": null, "e2": 429 },
           "f": ["bar", 404, false, null, {}]
         }"""
        )
        val field: EnumAnnotated,
    )

    @Serializable
    @AvroProp("enums", "power")
    @AvroProp("countingAgain", """["three", "four"]""")
    private enum class EnumAnnotated {
        Red,

        @AvroEnumDefault
        Green,
        Blue,
    }
}