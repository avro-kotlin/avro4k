package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.AvroFixed
import com.github.avrokotlin.avro4k.internal.nullable
import com.github.avrokotlin.avro4k.schema
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.Contextual
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerializationException
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import java.util.UUID

internal class UUIDSchemaTest : FunSpec({
    test("support UUID logical types as strings") {
        AvroAssertions.assertThat<UUIDTest>()
            .generatesSchema(LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING)))
    }

    test("support nullable UUID logical types as strings") {
        AvroAssertions.assertThat<UUIDNullableTest>()
            .generatesSchema(LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING)).nullable)
    }

    test("support UUID logical types as fixed") {
        AvroAssertions.assertThat<UUIDTestFixed>()
            .generatesSchema(LogicalTypes.uuid().addToSchema(Schema.createFixed("com.github.avrokotlin.avro4k.schema.UUIDSchemaTest.uuidFixed", null, null, 16)))
    }

    test("fails when @AvroFixed has bad size") {
        shouldThrow<SerializationException> {
            Avro.schema<UUIDTestFailingFixed>()
        }
    }
}) {
    @JvmInline
    @Serializable
    private value class UUIDTest(
        @Contextual val uuid: UUID,
    )

    @JvmInline
    @Serializable
    private value class UUIDTestFixed(
        @Contextual @AvroFixed(16) val uuidFixed: UUID,
    )

    @JvmInline
    @Serializable
    private value class UUIDTestFailingFixed(
        @Contextual @AvroFixed(56) val uuid: UUID,
    )

    @JvmInline
    @Serializable
    private value class UUIDNullableTest(
        @Contextual val uuid: UUID?,
    )
}