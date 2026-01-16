@file:OptIn(kotlin.uuid.ExperimentalUuidApi::class)

package com.github.avrokotlin.avro4k.encoding

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.AvroFixed
import com.github.avrokotlin.avro4k.encodeToBytesUsingApacheLib
import com.github.avrokotlin.avro4k.record
import com.github.avrokotlin.avro4k.schema
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import kotlin.uuid.Uuid
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToByteArray

internal class KotlinUuidEncodingTest : StringSpec({
    val uuid = Uuid.parse("123e4567-e89b-12d3-a456-426614174000")
    val uuidString = "123e4567-e89b-12d3-a456-426614174000"

    "encode kotlin.uuid.Uuid as string" {
        val schema = Avro.schema<Uuid>()
        val encoded = Avro.encodeToByteArray(uuid)
        val expectedBytes = encodeToBytesUsingApacheLib(schema, uuidString)

        encoded shouldBe expectedBytes
    }

    "encode nullable kotlin.uuid.Uuid" {
        val schema = Avro.schema<Uuid?>()

        // With value
        val encodedWithValue = Avro.encodeToByteArray(uuid as Uuid?)
        val expectedWithValue = encodeToBytesUsingApacheLib(schema, uuidString)
        encodedWithValue shouldBe expectedWithValue

        // With null
        val encodedWithNull = Avro.encodeToByteArray(null as Uuid?)
        val expectedWithNull = encodeToBytesUsingApacheLib(schema, null)
        encodedWithNull shouldBe expectedWithNull
    }

    "encode kotlin.uuid.Uuid in array" {
        val uuid2 = Uuid.parse("123e4567-e89b-12d3-a456-426614174001")
        val data = listOf(uuid, uuid2)

        val schema = Avro.schema<List<Uuid>>()
        val encoded = Avro.encodeToByteArray(data)
        val expectedBytes = encodeToBytesUsingApacheLib(schema, listOf(uuidString, uuid2.toString()))

        encoded shouldBe expectedBytes
    }

    "encode kotlin.uuid.Uuid in record" {
        val uuid2 = Uuid.parse("123e4567-e89b-12d3-a456-426614174001")

        AvroAssertions.assertThat(KotlinUuidRecord(uuid, uuid2))
            .isEncodedAs(record(uuidString, uuid2.toByteArray()))
    }
}) {
    @Serializable
    private data class KotlinUuidRecord(
        val uuidString: Uuid,
        @AvroFixed(16) val uuidFixed: Uuid,
    )
}
