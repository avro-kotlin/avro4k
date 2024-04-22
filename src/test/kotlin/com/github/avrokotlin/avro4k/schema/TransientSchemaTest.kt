package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.AvroAssertions
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder

class TransientSchemaTest : FunSpec({
    test("ignore fields with @Transient") {
        AvroAssertions.assertThat<TransientTest>()
            .generatesSchema(
                SchemaBuilder.record("TransientTest").fields()
                    .name("presentField").type(Schema.create(Schema.Type.STRING)).noDefault()
                    .endRecord()
            )
    }
}) {
    @Serializable
    @SerialName("TransientTest")
    private data class TransientTest(
        val presentField: String,
        @Transient val transientField: String = "default",
    )
}