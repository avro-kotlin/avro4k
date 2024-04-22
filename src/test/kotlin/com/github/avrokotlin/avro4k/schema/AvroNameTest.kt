package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.AvroAssertions
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.io.path.Path

class AvroNameSchemaTest : FunSpec({

    test("Change field and class name") {
        AvroAssertions.assertThat<FieldNamesFoo>()
            .generatesSchema(Path("/avro_name_field.json"))
    }
}) {
    @Serializable
    @SerialName("anotherRecordName")
    private data class FieldNamesFoo(
        @SerialName("foo") val wibble: String,
    )
}