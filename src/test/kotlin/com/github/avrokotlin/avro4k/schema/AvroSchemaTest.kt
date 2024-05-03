package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.AnnotatedLocation
import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.AvroSchema
import com.github.avrokotlin.avro4k.AvroSchemaSupplier
import io.kotest.core.spec.style.StringSpec
import kotlinx.serialization.Serializable
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder

internal class AvroSchemaTest : StringSpec({
    "@AvroLogicalType annotation should be supported" {
        AvroAssertions.assertThat<Something>()
            .generatesSchema(SchemaBuilder.fixed("myCustomSchema").doc("a doc").size(42))
    }
}) {
    @JvmInline
    @Serializable
    private value class Something(
        @AvroSchema(CustomSchemaSupplier::class) val value: String,
    )
}

internal object CustomSchemaSupplier : AvroSchemaSupplier {
    override fun getSchema(stack: List<AnnotatedLocation>): Schema {
        return Schema.createFixed("myCustomSchema", "a doc", null, 42)
    }
}