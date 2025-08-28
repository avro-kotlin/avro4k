package com.github.avrokotlin.avro4k.kafka.confluent

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroFixed
import com.github.avrokotlin.avro4k.encodeToByteArray
import com.github.avrokotlin.avro4k.schema
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerializationException
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import java.nio.ByteBuffer

class GenericKSerializerTest : StringSpec() {
    init {
        "Serializing unknown type should fail" {
            val schema = Schema.create(Schema.Type.STRING)

            class UnregisteredType

            val value = UnregisteredType()

            shouldThrow<SerializationException> { Avro.encodeToByteArray(schema, GenericKSerializer(), value) }
        }
        "GenericEnumSymbol should be serialized as ENUM" {
            val schema = Schema.createEnum("theEnum", null, null, listOf("A", "B", "C"))
            val value = GenericData.EnumSymbol(schema, "B")

            Avro.encodeToByteArray(schema, GenericKSerializer(), value) shouldBe
                byteArrayOf(2) // == 1 in zig-zag encoding
        }
        "GenericEnumSymbol should be serialized as ENUM even with a different name but same as alias in union" {
            val union =
                Schema.createUnion(
                    listOf(
                        Schema.create(Schema.Type.STRING),
                        Schema.createEnum("theEnum", null, null, listOf("A", "B", "C"))
                    )
                )
            val value = GenericData.EnumSymbol(Schema.createEnum("unknownEnum", null, null, listOf("A", "B", "C")).also { it.addAlias("theEnum") }, "B")

            Avro.encodeToByteArray(union, GenericKSerializer(), value) shouldBe
                byteArrayOf(2, 2) // == 1 in zig-zag encoding
        }
        "ENUM should be deserialized as GenericEnumSymbol" {
            val schema = Schema.createEnum("theEnum", null, null, listOf("A", "B", "C"))

            Avro.decodeFromByteArray(schema, GenericKSerializer(), byteArrayOf(2)) shouldBe GenericData.EnumSymbol(schema, "B")
        }
        "Utf8 should be serialized as STRING" {
            val schema = Schema.create(Schema.Type.STRING)
            val value = Utf8("Hello")

            Avro.encodeToByteArray(schema, GenericKSerializer(), value) shouldBe
                Avro.encodeToByteArray(schema, "Hello")
        }
        "STRING should be deserialized as String" {
            val schema = Schema.create(Schema.Type.STRING)
            val bytes = Avro.encodeToByteArray(schema, "Hello")

            Avro.decodeFromByteArray(schema, GenericKSerializer(), bytes) shouldBe "Hello"
        }
        "ByteBuffer should be serialized as BYTES" {
            val schema = Schema.create(Schema.Type.BYTES)
            val value = ByteBuffer.wrap(byteArrayOf(1, 2, 3)).position(1)

            Avro.encodeToByteArray(schema, GenericKSerializer(), value) shouldBe
                Avro.encodeToByteArray(schema, byteArrayOf(2, 3))
        }
        "BYTES should be deserialized as ByteArray" {
            val schema = Schema.create(Schema.Type.BYTES)
            val bytes = Avro.encodeToByteArray(schema, byteArrayOf(1, 2, 3))

            Avro.decodeFromByteArray(schema, GenericKSerializer(), bytes) shouldBe byteArrayOf(1, 2, 3)
        }
        "ByteBuffer should be serialized as FIXED" {
            val schema = Schema.createFixed("theFixed", null, null, 3)
            val value = ByteBuffer.wrap(byteArrayOf(1, 2, 3))

            Avro.encodeToByteArray(schema, GenericKSerializer(), value) shouldBe
                Avro.encodeToByteArray(schema, byteArrayOf(1, 2, 3))
        }
        "ByteBuffer at custom position should be serialized as FIXED only the bytes after the position" {
            val schema = Schema.createFixed("theFixed", null, null, 2)
            val value = ByteBuffer.wrap(byteArrayOf(1, 2, 3)).position(1)

            Avro.encodeToByteArray(schema, GenericKSerializer(), value) shouldBe
                Avro.encodeToByteArray(schema, byteArrayOf(2, 3))
        }
        "GenericFixed should be serialized as FIXED" {
            val schema = Schema.createFixed("theFixed", null, null, 3)
            val value = GenericData.Fixed(schema, byteArrayOf(1, 2, 3))

            Avro.encodeToByteArray(schema, GenericKSerializer(), value) shouldBe
                Avro.encodeToByteArray(schema, byteArrayOf(1, 2, 3))
        }
        "GenericFixed should be serialized as FIXED even with a different name but same as alias in union" {
            val schema =
                Schema.createUnion(
                    listOf(
                        Schema.create(Schema.Type.BYTES),
                        Schema.createFixed("theFixed", null, null, 3)
                    )
                )
            val value = GenericData.Fixed(Schema.createFixed("unknownFixed", null, null, 3).also { it.addAlias("theFixed") }, byteArrayOf(1, 2, 3))

            Avro.encodeToByteArray(schema, GenericKSerializer(), value) shouldBe
                byteArrayOf(2, 1, 2, 3) // 2 == 1 in zig-zag encoding, followed by the fixed bytes
        }
        "FIXED should be deserialized as GenericFixed" {
            val schema = Schema.createFixed("theFixed", null, null, 3)
            val bytes = byteArrayOf(1, 2, 3)

            Avro.decodeFromByteArray(schema, GenericKSerializer(), bytes) shouldBe GenericData.Fixed(schema, byteArrayOf(1, 2, 3))
        }

        "GenericRecord should be serialized as RECORD" {
            @Serializable
            data class TheRecord(val field1: String, val field2: Int)

            val schema = Avro.schema<TheRecord>()
            val value =
                GenericData.Record(schema).apply {
                    put("field1", "Hello")
                    put("field2", 42)
                }

            Avro.encodeToByteArray(schema, GenericKSerializer(), value) shouldBe
                Avro.encodeToByteArray(schema, TheRecord("Hello", 42))
        }
        "GenericRecord should be serialized as RECORD even with a different name but same as alias in union" {
            @Serializable
            @SerialName("theRecord")
            data class TheRecord(val field1: String, val field2: Int)
            val schema =
                Schema.createUnion(
                    Schema.create(Schema.Type.INT),
                    Avro.schema<TheRecord>()
                )

            val value =
                GenericData.Record(
                    SchemaBuilder.record("unknownRecord")
                        .aliases("theRecord")
                        .fields()
                        .requiredString("field1")
                        .name("unknownField").aliases("field2").type().intType().noDefault()
                        .endRecord()
                ).apply {
                    put("field1", "Hello")
                    put("unknownField", 42)
                }

            Avro.encodeToByteArray(schema, GenericKSerializer(), value) shouldBe
                Avro.encodeToByteArray(schema, TheRecord("Hello", 42))
        }
        "RECORD should be deserialized as GenericRecord" {
            @Serializable
            data class TheRecord(
                val field1: String,
                val field2: Int?,
                @AvroFixed(3) val fixed: ByteArray,
            )

            val schema = Avro.schema<TheRecord>()
            val bytes = Avro.encodeToByteArray(schema, TheRecord("Hello", null, byteArrayOf(1, 2, 3)))

            Avro.decodeFromByteArray(schema, GenericKSerializer(), bytes) shouldBe
                GenericData.Record(schema).apply {
                    put("field1", "Hello")
                    put("field2", null)
                    // nested generic data works
                    put("fixed", GenericData.Fixed(Schema.createFixed("fixed", null, null, 3), byteArrayOf(1, 2, 3)))
                }
        }
    }
}