package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.AvroDecoder
import com.github.avrokotlin.avro4k.AvroEncoder
import com.github.avrokotlin.avro4k.internal.nullable
import com.github.avrokotlin.avro4k.serializer.AvroSerializer
import com.github.avrokotlin.avro4k.serializer.SchemaSupplierContext
import io.kotest.core.spec.style.StringSpec
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder

internal class AvroCustomSchemaTest : StringSpec({
    "support custom schema" {
        AvroAssertions.assertThat<CustomSchemaValueClass>()
            .generatesSchema(CustomSchemaSerializer.SCHEMA)
        AvroAssertions.assertThat<CustomSchemaValueClass?>()
            .generatesSchema(CustomSchemaSerializer.SCHEMA.nullable)
        AvroAssertions.assertThat<CustomSchemaClass>()
            .generatesSchema(
                SchemaBuilder.record("CustomSchemaClass")
                    .fields()
                    .name("value").type(CustomSchemaSerializer.SCHEMA).noDefault()
                    .name("nullableValue").type(CustomSchemaSerializer.SCHEMA.nullable).withDefault(null)
                    .endRecord()
            )
    }
}) {
    @JvmInline
    @Serializable
    private value class CustomSchemaValueClass(
        @Serializable(with = CustomSchemaSerializer::class) val value: String,
    )

    @Serializable
    @SerialName("CustomSchemaClass")
    private data class CustomSchemaClass(
        @Serializable(with = CustomSchemaSerializer::class) val value: String,
        @Serializable(with = CustomSchemaSerializer::class) val nullableValue: String?,
    )

    private object CustomSchemaSerializer : AvroSerializer<String>("CustomSchema") {
        val SCHEMA = Schema.createUnion(Schema.createFixed("testFixed", "doc", "namespace", 10))

        override fun getSchema(context: SchemaSupplierContext): Schema {
            return SCHEMA
        }

        override fun serializeAvro(
            encoder: AvroEncoder,
            value: String,
        ) {
            TODO("Not yet implemented")
        }

        override fun deserializeAvro(decoder: AvroDecoder): String {
            TODO("Not yet implemented")
        }
    }
}