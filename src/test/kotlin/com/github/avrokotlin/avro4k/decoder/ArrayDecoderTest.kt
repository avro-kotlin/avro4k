package com.github.avrokotlin.avro4k.decoder

import com.github.avrokotlin.avro4k.Avro
import io.kotest.core.spec.style.WordSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

@Serializable
data class TestArrayBooleans(val booleans: Array<Boolean>)

@Serializable
data class TestListDoubles(val doubles: List<Double>)

@Serializable
data class TestSetString(val strings: Set<String>)

@Serializable
data class TestArrayRecords(val records: Array<Record>)

@Serializable
data class TestListRecords(val records: List<Record>)

@Serializable
data class TestSetRecords(val records: Set<Record>)

@Serializable
data class Record(val str: String, val double: Double)

class ArrayDecoderTest : WordSpec({

    "Decoder" should {
        listOf(
            "array" to arrayOf(true, false, true),
            "list" to listOf(true, false, true),
            "GenericData.Array" to GenericData.Array(
                Schema.createArray(Schema.create(Schema.Type.BOOLEAN)), listOf(true, false, true)
            )
        ).forEach {
            "support ${it.first} for an Array of booleans" {
                val schema = Avro.default.schema(TestArrayBooleans.serializer())
                val record = GenericData.Record(schema)
                record.put("booleans", it.second)
                Avro.default.fromRecord(TestArrayBooleans.serializer(), record).booleans.toList() shouldBe listOf(
                    true, false, true
                )
            }
        }
        listOf(
            "array" to arrayOf(12.54, 23.5, 9123.2314),
            "list" to listOf(12.54, 23.5, 9123.2314),
            "GenericData.Array" to GenericData.Array(
                Schema.createArray(Schema.create(Schema.Type.DOUBLE)), listOf(12.54, 23.5, 9123.2314)
            )
        ).forEach {
            "support ${it.first} for a List of doubles" {
                val schema = Avro.default.schema(TestListDoubles.serializer())
                val record = GenericData.Record(schema)
                record.put("doubles", it.second)
                Avro.default.fromRecord(TestListDoubles.serializer(), record) shouldBe TestListDoubles(
                    listOf(
                        12.54, 23.5, 9123.2314
                    )
                )
            }
        }
        val recordSchema = Avro.default.schema(Record.serializer())
        val records = listOf(GenericData.Record(recordSchema).apply {
            put("str", "qwe")
            put("double", 123.4)
        }, GenericData.Record(recordSchema).apply {
            put("str", "wer")
            put("double", 8234.324)
        })
        listOf(
            "array" to records.toTypedArray(),
            "list" to records,
            "GenericData.Array" to GenericData.Array(
                Schema.createArray(recordSchema), records
            )
        ).forEach {
            "support ${it.first} for a List of records" {
                val containerSchema = Avro.default.schema(TestListRecords.serializer())
                val container = GenericData.Record(containerSchema)
                container.put("records", it.second)

                Avro.default.fromRecord(
                    TestListRecords.serializer(), container
                ) shouldBe TestListRecords(listOf(Record("qwe", 123.4), Record("wer", 8234.324)))
            }
            "support ${it.first} for a Set of records" {
                val containerSchema = Avro.default.schema(TestSetRecords.serializer())
                val container = GenericData.Record(containerSchema)
                container.put("records", it.second)

                Avro.default.fromRecord(
                    TestSetRecords.serializer(), container
                ) shouldBe TestSetRecords(setOf(Record("qwe", 123.4), Record("wer", 8234.324)))
            }
        }

        listOf(
            "array" to arrayOf("Qwe", "324", "q"),
            "list" to listOf("Qwe", "324", "q"),
            "GenericData.Array" to GenericData.Array(
                Schema.createArray(Schema.create(Schema.Type.STRING)), listOf("Qwe", "324", "q")
            )
        ).forEach {
            "support ${it.first} for a Set of strings" {
                val schema = Avro.default.schema(TestSetString.serializer())
                val record = GenericData.Record(schema)
                record.put("strings", it.second)
                Avro.default.fromRecord(TestSetString.serializer(), record) shouldBe TestSetString(setOf("Qwe", "324", "q"))
            }
        }
    }

})