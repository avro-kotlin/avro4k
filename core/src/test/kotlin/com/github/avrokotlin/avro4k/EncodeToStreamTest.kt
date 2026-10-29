package com.github.avrokotlin.avro4k

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToByteArray
import org.apache.avro.SchemaNormalization
import java.io.ByteArrayOutputStream

internal class EncodeToStreamTest : StringSpec({
    val value = SimpleRecord("alice", 42)

    "encodeToStream writes the same non-empty bytes as encodeToByteArray" {
        val expected = Avro.encodeToByteArray(value)
        val output = ByteArrayOutputStream()

        Avro.encodeToStream(value, output)

        output.size() shouldBe expected.size
        output.toByteArray() shouldBe expected
    }

    "encodeToStream with writer schema and serializer writes the same bytes as encodeToByteArray" {
        val serializer = SimpleRecord.serializer()
        val schema = Avro.schema(serializer)
        val expected = Avro.encodeToByteArray(schema, serializer, value)
        val output = ByteArrayOutputStream()

        Avro.encodeToStream(schema, serializer, value, output)

        output.toByteArray() shouldBe expected
    }

    "encodeToStream with writer schema writes the same bytes as encodeToByteArray" {
        val schema = Avro.schema<SimpleRecord>()
        val expected = Avro.encodeToByteArray(schema, value)
        val output = ByteArrayOutputStream()

        Avro.encodeToStream(schema, value, output)

        output.toByteArray() shouldBe expected
    }

    "AvroSingleObject.encodeToStream writes the same non-empty bytes as encodeToByteArray" {
        val serializer = SimpleRecord.serializer()
        val schema = Avro.schema(serializer)
        val schemas = mapOf(SchemaNormalization.parsingFingerprint64(schema) to schema)
        val avroSingleObject = AvroSingleObject(schemas::get)
        val expected = avroSingleObject.encodeToByteArray(schema, serializer, value)
        val output = ByteArrayOutputStream()

        avroSingleObject.encodeToStream(schema, serializer, value, output)

        output.size() shouldBe expected.size
        output.toByteArray() shouldBe expected
    }
}) {
    @Serializable
    private data class SimpleRecord(
        val name: String,
        val age: Int,
    )
}