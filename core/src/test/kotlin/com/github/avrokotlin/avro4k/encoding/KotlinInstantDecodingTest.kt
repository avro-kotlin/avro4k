package com.github.avrokotlin.avro4k.encoding

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroStringable
import com.github.avrokotlin.avro4k.decodeFromByteArray
import com.github.avrokotlin.avro4k.encodeToBytesUsingApacheLib
import com.github.avrokotlin.avro4k.schema
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import kotlinx.datetime.Instant
import kotlinx.serialization.Serializable
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData

internal class KotlinInstantDecodingTest : StringSpec({
    // 2020-01-01T12:34:56Z
    val instant = Instant.fromEpochSeconds(1577882096)
    val instantMillis = 1577882096000L

    // 2020-01-01T12:34:56.000424Z (with microseconds)
    val instantWithMicros = Instant.fromEpochSeconds(1577882096, 424000)
    val instantMicros = 1577882096000424L

    // 2020-01-01T12:34:56.000000424Z (with nanoseconds)
    val instantWithNanos = Instant.fromEpochSeconds(1577882096, 424)
    val instantNanos = 1577882096000000424L

    "decode kotlinx.datetime.Instant from timestamp-millis" {
        val schema = SchemaBuilder.builder().longType()
        LogicalTypes.timestampMillis().addToSchema(schema)

        val bytes = encodeToBytesUsingApacheLib(schema, instantMillis)
        val decoded = Avro.decodeFromByteArray<Instant>(schema, bytes)

        decoded shouldBe instant
    }

    "decode nullable kotlinx.datetime.Instant" {
        val schema = Avro.schema<Instant?>()

        // With value
        val bytesWithValue = encodeToBytesUsingApacheLib(schema, instantMillis)
        val decodedWithValue = Avro.decodeFromByteArray<Instant?>(schema, bytesWithValue)
        decodedWithValue shouldBe instant

        // With null
        val bytesWithNull = encodeToBytesUsingApacheLib(schema, null)
        val decodedWithNull = Avro.decodeFromByteArray<Instant?>(schema, bytesWithNull)
        decodedWithNull shouldBe null
    }

    "decode kotlinx.datetime.Instant from timestamp-micros" {
        val schema = SchemaBuilder.builder().longType()
        LogicalTypes.timestampMicros().addToSchema(schema)

        val bytes = encodeToBytesUsingApacheLib(schema, instantMicros)
        val decoded = Avro.decodeFromByteArray<Instant>(schema, bytes)

        decoded shouldBe instantWithMicros
    }

    "decode kotlinx.datetime.Instant from timestamp-nanos" {
        val schema = SchemaBuilder.builder().longType()
        LogicalTypes.timestampNanos().addToSchema(schema)

        val bytes = encodeToBytesUsingApacheLib(schema, instantNanos)
        val decoded = Avro.decodeFromByteArray<Instant>(schema, bytes)

        decoded shouldBe instantWithNanos
    }

    "decode kotlinx.datetime.Instant from string" {
        val schema = Schema.create(Schema.Type.STRING)

        val bytes = encodeToBytesUsingApacheLib(schema, instant.toString())
        val decoded = Avro.decodeFromByteArray<Instant>(schema, bytes)

        decoded shouldBe instant
    }

    "decode kotlinx.datetime.Instant from long without logical type" {
        val schema = Schema.create(Schema.Type.LONG)

        val bytes = encodeToBytesUsingApacheLib(schema, instantMillis)
        val decoded = Avro.decodeFromByteArray<Instant>(schema, bytes)

        decoded shouldBe instant
    }

    "decode kotlinx.datetime.Instant from array" {
        val instant2 = Instant.fromEpochSeconds(1577882097)
        val schema = Avro.schema<List<Instant>>()

        val bytes = encodeToBytesUsingApacheLib(schema, listOf(instantMillis, 1577882097000L))
        val decoded = Avro.decodeFromByteArray<List<Instant>>(schema, bytes)

        decoded shouldBe listOf(instant, instant2)
    }

    "decode kotlinx.datetime.Instant from record" {
        val schema = Avro.schema<InstantRecord>()
        val bytes = encodeToBytesUsingApacheLib(
            schema,
            GenericData.Record(schema).apply {
                put(0, instantMillis)
                put(1, instant.toString())
            }
        )

        val decoded = Avro.decodeFromByteArray<InstantRecord>(schema, bytes)

        decoded shouldBe InstantRecord(instant, instant.toString())
    }
}) {
    @Serializable
    private data class InstantRecord(
        val instant: Instant,
        @AvroStringable val instantString: String,
    )
}