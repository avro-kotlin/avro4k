@file:OptIn(kotlin.uuid.ExperimentalUuidApi::class)

package com.github.avrokotlin.avro4k.encoding

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroFixed
import com.github.avrokotlin.avro4k.decodeFromByteArray
import com.github.avrokotlin.avro4k.encodeToBytesUsingApacheLib
import com.github.avrokotlin.avro4k.schema
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import kotlin.uuid.Uuid
import kotlinx.serialization.Serializable
import org.apache.avro.generic.GenericData

internal class KotlinUuidDecodingTest : StringSpec({
    val uuid = Uuid.parse("123e4567-e89b-12d3-a456-426614174000")
    val uuidString = "123e4567-e89b-12d3-a456-426614174000"

    "decode kotlin.uuid.Uuid from string" {
        val schema = Avro.schema<Uuid>()
        val bytes = encodeToBytesUsingApacheLib(schema, uuidString)

        val decoded = Avro.decodeFromByteArray<Uuid>(schema, bytes)

        decoded shouldBe uuid
    }

    "decode nullable kotlin.uuid.Uuid" {
        val schema = Avro.schema<Uuid?>()

        // With value
        val bytesWithValue = encodeToBytesUsingApacheLib(schema, uuidString)
        val decodedWithValue = Avro.decodeFromByteArray<Uuid?>(schema, bytesWithValue)
        decodedWithValue shouldBe uuid

        // With null
        val bytesWithNull = encodeToBytesUsingApacheLib(schema, null)
        val decodedWithNull = Avro.decodeFromByteArray<Uuid?>(schema, bytesWithNull)
        decodedWithNull shouldBe null
    }

    "decode kotlin.uuid.Uuid from array" {
        val uuid2 = Uuid.parse("123e4567-e89b-12d3-a456-426614174001")
        val schema = Avro.schema<List<Uuid>>()
        val bytes = encodeToBytesUsingApacheLib(schema, listOf(uuidString, uuid2.toString()))

        val decoded = Avro.decodeFromByteArray<List<Uuid>>(schema, bytes)

        decoded shouldBe listOf(uuid, uuid2)
    }

    "decode kotlin.uuid.Uuid from record" {
        val uuid2 = Uuid.parse("123e4567-e89b-12d3-a456-426614174001")
        val uuid2Bytes = uuid2.toByteArray()

        val schema = Avro.schema<KotlinUuidRecord>()
        val fixedSchema = schema.fields[1].schema()
        val bytes = encodeToBytesUsingApacheLib(schema, GenericData.Record(schema).apply {
            put(0, uuidString)
            put(1, GenericData.Fixed(fixedSchema, uuid2Bytes))
        })

        val decoded = Avro.decodeFromByteArray<KotlinUuidRecord>(schema, bytes)

        decoded shouldBe KotlinUuidRecord(uuid, uuid2)
    }
}) {
    @Serializable
    private data class KotlinUuidRecord(
        val uuidString: Uuid,
        @AvroFixed(16) val uuidFixed: Uuid,
    )
}
