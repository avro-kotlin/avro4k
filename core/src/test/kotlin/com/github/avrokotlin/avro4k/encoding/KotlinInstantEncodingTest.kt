@file:OptIn(kotlin.time.ExperimentalTime::class)

package com.github.avrokotlin.avro4k.encoding

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroAssertions
import com.github.avrokotlin.avro4k.AvroStringable
import com.github.avrokotlin.avro4k.encodeToByteArray
import com.github.avrokotlin.avro4k.encodeToBytesUsingApacheLib
import com.github.avrokotlin.avro4k.record
import com.github.avrokotlin.avro4k.schema
import com.github.avrokotlin.avro4k.serializer.LOGICAL_TYPE_NAME_TIMESTAMP_MICROS
import com.github.avrokotlin.avro4k.serializer.LOGICAL_TYPE_NAME_TIMESTAMP_MILLIS
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerializationException
import kotlinx.serialization.encodeToByteArray
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import kotlin.time.Instant

internal class KotlinInstantEncodingTest : StringSpec({
    // 2020-01-01T12:34:56Z
    val instant = Instant.fromEpochSeconds(1577882096)
    val instantMillis = 1577882096000L

    // 2020-01-01T12:34:56.000424Z (with microseconds)
    val instantWithMicros = Instant.fromEpochSeconds(1577882096, 424000)
    val instantMicros = 1577882096000424L

    // 2020-01-01T12:34:56.000000424Z (with nanoseconds)
    val instantWithNanos = Instant.fromEpochSeconds(1577882096, 424)
    val instantNanos = 1577882096000000424L

    val preEpochFractionalInstant = Instant.fromEpochSeconds(-1, 123_456_789)
    val preEpochMillis = -877L
    val preEpochMicros = -876_544L
    val preEpochNanos = -876_543_211L

    "encode kotlin.time.Instant as timestamp-millis" {
        val schema = SchemaBuilder.builder().longType()
        LogicalTypes.timestampMillis().addToSchema(schema)

        val encoded = Avro.encodeToByteArray(instant)
        val expectedBytes = encodeToBytesUsingApacheLib(schema, instantMillis)

        encoded shouldBe expectedBytes
    }

    "encode nullable kotlin.time.Instant" {
        val schema = Avro.schema<Instant?>()

        // With value
        val encodedWithValue = Avro.encodeToByteArray(instant as Instant?)
        val expectedWithValue = encodeToBytesUsingApacheLib(schema, instantMillis)
        encodedWithValue shouldBe expectedWithValue

        // With null
        val encodedWithNull = Avro.encodeToByteArray(null as Instant?)
        val expectedWithNull = encodeToBytesUsingApacheLib(schema, null)
        encodedWithNull shouldBe expectedWithNull
    }

    "encode kotlin.time.Instant as timestamp-micros using writer schema" {
        val schema = SchemaBuilder.builder().longType()
        LogicalTypes.timestampMicros().addToSchema(schema)

        val encoded = Avro.encodeToByteArray<Instant>(schema, instantWithMicros)
        val expectedBytes = encodeToBytesUsingApacheLib(schema, instantMicros)

        encoded shouldBe expectedBytes
    }

    "encode kotlin.time.Instant as timestamp-nanos using writer schema" {
        val schema = SchemaBuilder.builder().longType()
        LogicalTypes.timestampNanos().addToSchema(schema)

        val encoded = Avro.encodeToByteArray<Instant>(schema, instantWithNanos)
        val expectedBytes = encodeToBytesUsingApacheLib(schema, instantNanos)

        encoded shouldBe expectedBytes
    }

    "encode pre-epoch fractional kotlin.time.Instant as timestamp-millis" {
        val schema = SchemaBuilder.builder().longType()
        LogicalTypes.timestampMillis().addToSchema(schema)

        val encoded = Avro.encodeToByteArray<Instant>(schema, preEpochFractionalInstant)
        val expectedBytes = encodeToBytesUsingApacheLib(schema, preEpochMillis)

        encoded shouldBe expectedBytes
    }

    "encode pre-epoch fractional kotlin.time.Instant as timestamp-micros" {
        val schema = SchemaBuilder.builder().longType()
        LogicalTypes.timestampMicros().addToSchema(schema)

        val encoded = Avro.encodeToByteArray<Instant>(schema, preEpochFractionalInstant)
        val expectedBytes = encodeToBytesUsingApacheLib(schema, preEpochMicros)

        encoded shouldBe expectedBytes
    }

    "encode pre-epoch fractional kotlin.time.Instant as timestamp-nanos" {
        val schema = SchemaBuilder.builder().longType()
        LogicalTypes.timestampNanos().addToSchema(schema)

        val encoded = Avro.encodeToByteArray<Instant>(schema, preEpochFractionalInstant)
        val expectedBytes = encodeToBytesUsingApacheLib(schema, preEpochNanos)

        encoded shouldBe expectedBytes
    }

    "reject kotlin.time.Instant outside timestamp-nanos range" {
        val schema = SchemaBuilder.builder().longType()
        LogicalTypes.timestampNanos().addToSchema(schema)
        val outOfRangeInstant = Instant.parse("2262-04-12T00:00:00Z")

        shouldThrow<SerializationException> {
            Avro.encodeToByteArray<Instant>(schema, outOfRangeInstant)
        }
    }

    "reject kotlin.time.Instant outside timestamp-millis range" {
        val schema = SchemaBuilder.builder().longType()
        LogicalTypes.timestampMillis().addToSchema(schema)
        val outOfRangeInstant = Instant.fromEpochSeconds(Long.MAX_VALUE / 1_000L + 1)

        shouldThrow<SerializationException> {
            Avro.encodeToByteArray<Instant>(schema, outOfRangeInstant)
        }
    }

    "reject kotlin.time.Instant outside timestamp-micros range" {
        val schema = SchemaBuilder.builder().longType()
        LogicalTypes.timestampMicros().addToSchema(schema)
        val outOfRangeInstant = Instant.fromEpochSeconds(Long.MAX_VALUE / 1_000_000L + 1)

        shouldThrow<SerializationException> {
            Avro.encodeToByteArray<Instant>(schema, outOfRangeInstant)
        }
    }

    "encode kotlin.time.Instant as string using @AvroStringable" {
        AvroAssertions.assertThat(InstantStringable(instant))
            .isEncodedAs(record(instant.toString()))
    }

    "encode kotlin.time.Instant in array" {
        val instant2 = Instant.fromEpochSeconds(1577882097)
        val data = listOf(instant, instant2)

        val schema = Avro.schema<List<Instant>>()
        val encoded = Avro.encodeToByteArray(data)
        val expectedBytes = encodeToBytesUsingApacheLib(schema, listOf(instantMillis, 1577882097000L))

        encoded shouldBe expectedBytes
    }

    "encode kotlin.time.Instant in record" {
        AvroAssertions.assertThat(InstantRecord(instant, instant.toString()))
            .isEncodedAs(record(instantMillis, instant.toString()))
    }

    "generate schema with timestamp-millis logical type" {
        val schema = Avro.schema<Instant>()

        schema.type shouldBe Schema.Type.LONG
        schema.logicalType.name shouldBe LOGICAL_TYPE_NAME_TIMESTAMP_MILLIS
    }

    "generate schema for record with Instant fields" {
        val schema = Avro.schema<InstantRecord>()

        val instantField = schema.getField("instant")
        instantField.schema().type shouldBe Schema.Type.LONG
        instantField.schema().logicalType.name shouldBe LOGICAL_TYPE_NAME_TIMESTAMP_MILLIS

        val instantStringField = schema.getField("instantString")
        instantStringField.schema().type shouldBe Schema.Type.STRING
    }
}) {
    @Serializable
    private data class InstantStringable(
        @AvroStringable val instant: Instant,
    )

    @Serializable
    private data class InstantRecord(
        val instant: Instant,
        @AvroStringable val instantString: String,
    )
}