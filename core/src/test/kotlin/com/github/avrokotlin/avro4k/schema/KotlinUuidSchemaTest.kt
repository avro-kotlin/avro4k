@file:OptIn(kotlin.uuid.ExperimentalUuidApi::class)

package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.AvroFixed
import com.github.avrokotlin.avro4k.internal.nullable
import com.github.avrokotlin.avro4k.schema
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerializationException
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import kotlin.uuid.Uuid

internal class KotlinUuidSchemaTest : FunSpec({
    test("support kotlin.uuid.Uuid schema as string") {
        Avro.schema<Uuid>() shouldBe LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING))
    }

    test("support nullable kotlin.uuid.Uuid schema as string") {
        Avro.schema<Uuid?>() shouldBe LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING)).nullable
    }

    test("support kotlin.uuid.Uuid logical types as strings") {
        AvroAssertions.assertThat<KotlinUuidTest>()
            .generatesSchema(LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING)))
    }

    test("support nullable kotlin.uuid.Uuid logical types as strings") {
        AvroAssertions.assertThat<KotlinUuidNullableTest>()
            .generatesSchema(LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING)).nullable)
    }

    test("support kotlin.uuid.Uuid logical types as fixed") {
        AvroAssertions.assertThat<KotlinUuidTestFixed>()
            .generatesSchema(LogicalTypes.uuid().addToSchema(Schema.createFixed("com.github.avrokotlin.avro4k.schema.KotlinUuidSchemaTest.uuidFixed", null, null, 16)))
    }

    test("fails when @AvroFixed has bad size") {
        shouldThrow<SerializationException> {
            Avro.schema<KotlinUuidTestFailingFixed>()
        }
    }
}) {
    @JvmInline
    @Serializable
    private value class KotlinUuidTest(
        val uuid: Uuid,
    )

    @JvmInline
    @Serializable
    private value class KotlinUuidTestFixed(
        @AvroFixed(16) val uuidFixed: Uuid,
    )

    @JvmInline
    @Serializable
    private value class KotlinUuidTestFailingFixed(
        @AvroFixed(56) val uuid: Uuid,
    )

    @JvmInline
    @Serializable
    private value class KotlinUuidNullableTest(
        val uuid: Uuid?,
    )
}