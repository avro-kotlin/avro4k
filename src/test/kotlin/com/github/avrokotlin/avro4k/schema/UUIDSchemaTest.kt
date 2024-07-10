package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.internal.nullable
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.Contextual
import kotlinx.serialization.Serializable
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import java.util.UUID

internal class UUIDSchemaTest : FunSpec({
    test("support UUID logical types") {
        AvroAssertions.assertThat<UUIDTest>()
            .generatesSchema(LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING)))
    }

    test("support nullable UUID logical types") {
        AvroAssertions.assertThat<UUIDNullableTest>()
            .generatesSchema(LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING)).nullable)
    }
}) {
    @JvmInline
    @Serializable
    private value class UUIDTest(
        @Contextual val uuid: UUID,
    )

    @JvmInline
    @Serializable
    private value class UUIDNullableTest(
        @Contextual val uuid: UUID?,
    )
}