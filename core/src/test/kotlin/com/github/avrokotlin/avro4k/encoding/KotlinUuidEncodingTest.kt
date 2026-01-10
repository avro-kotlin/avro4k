@file:OptIn(kotlin.uuid.ExperimentalUuidApi::class)

package com.github.avrokotlin.avro4k.encoding

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.AvroFixed
import com.github.avrokotlin.avro4k.record
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import kotlin.uuid.Uuid
import kotlinx.serialization.Contextual
import kotlinx.serialization.Serializable

internal class KotlinUuidEncodingTest : StringSpec({
    "encode and decode kotlin.uuid.Uuid as string" {
        val uuid = Uuid.parse("123e4567-e89b-12d3-a456-426614174000")
        val data = KotlinUuidAsString(uuid)

        val encoded = Avro.encodeToByteArray(KotlinUuidAsString.serializer(), data)
        val decoded = Avro.decodeFromByteArray(KotlinUuidAsString.serializer(), encoded)

        decoded shouldBe data
    }

    "encode and decode kotlin.uuid.Uuid as fixed" {
        val uuid = Uuid.parse("123e4567-e89b-12d3-a456-426614174000")
        val data = KotlinUuidAsFixed(uuid)

        val encoded = Avro.encodeToByteArray(KotlinUuidAsFixed.serializer(), data)
        val decoded = Avro.decodeFromByteArray(KotlinUuidAsFixed.serializer(), encoded)

        decoded shouldBe data
    }

    "encode and decode nullable kotlin.uuid.Uuid" {
        val uuid = Uuid.parse("123e4567-e89b-12d3-a456-426614174000")
        val dataWithValue = KotlinUuidNullable(uuid)
        val dataWithNull = KotlinUuidNullable(null)

        val encodedWithValue = Avro.encodeToByteArray(KotlinUuidNullable.serializer(), dataWithValue)
        val decodedWithValue = Avro.decodeFromByteArray(KotlinUuidNullable.serializer(), encodedWithValue)
        decodedWithValue shouldBe dataWithValue

        val encodedWithNull = Avro.encodeToByteArray(KotlinUuidNullable.serializer(), dataWithNull)
        val decodedWithNull = Avro.decodeFromByteArray(KotlinUuidNullable.serializer(), encodedWithNull)
        decodedWithNull shouldBe dataWithNull
    }

    "support kotlin.uuid.Uuid in records" {
        val uuid1 = Uuid.parse("123e4567-e89b-12d3-a456-426614174000")
        val uuid2 = Uuid.parse("123e4567-e89b-12d3-a456-426614174001")

        AvroAssertions.assertThat(
            KotlinUuidRecord(uuid1, uuid2)
        ).isEncodedAs(
            record(
                "123e4567-e89b-12d3-a456-426614174000",
                uuid2.toByteArray()
            )
        )
    }
}) {
    @JvmInline
    @Serializable
    private value class KotlinUuidAsString(
        @Contextual val uuid: Uuid,
    )

    @JvmInline
    @Serializable
    private value class KotlinUuidAsFixed(
        @Contextual @AvroFixed(16) val uuid: Uuid,
    )

    @JvmInline
    @Serializable
    private value class KotlinUuidNullable(
        @Contextual val uuid: Uuid?,
    )

    @Serializable
    private data class KotlinUuidRecord(
        @Contextual val uuidString: Uuid,
        @Contextual @AvroFixed(16) val uuidFixed: Uuid,
    )
}
