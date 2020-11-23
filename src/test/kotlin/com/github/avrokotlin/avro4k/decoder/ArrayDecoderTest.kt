package com.github.avrokotlin.avro4k.decoder

import com.github.avrokotlin.avro4k.Avro
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.WordSpec
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

    "support array for an Array of booleans" {
      val schema = Avro.default.schema(TestArrayBooleans.serializer())
      val record = GenericData.Record(schema)
      record.put("booleans", arrayOf(true, false, true))
      Avro.default.fromRecord(TestArrayBooleans.serializer(), record).booleans.toList() shouldBe
          listOf(true, false, true)
    }

    "support list for an Array of booleans" {
      val schema = Avro.default.schema(TestArrayBooleans.serializer())
      val record = GenericData.Record(schema)
      record.put("booleans", listOf(true, false, true))
      Avro.default.fromRecord(TestArrayBooleans.serializer(), record).booleans.toList() shouldBe
          listOf(true, false, true)
    }

    "support GenericData.Array for an Array of booleans" {
      val schema = Avro.default.schema(TestArrayBooleans.serializer())
      val record = GenericData.Record(schema)
      record.put("booleans",
          GenericData.Array(Schema.createArray(Schema.create(Schema.Type.BOOLEAN)), listOf(true, false, true)))
      Avro.default.fromRecord(TestArrayBooleans.serializer(), record).booleans.toList() shouldBe
          listOf(true, false, true)
    }

    "support array for a List of doubles" {

      val schema = Avro.default.schema(TestListDoubles.serializer())
      val record = GenericData.Record(schema)
      record.put("doubles", arrayOf(12.54, 23.5, 9123.2314))
      Avro.default.fromRecord(TestListDoubles.serializer(), record) shouldBe
          TestListDoubles(listOf(12.54, 23.5, 9123.2314))
    }

    "support list for a List of doubles" {

      val schema = Avro.default.schema(TestListDoubles.serializer())
      val record = GenericData.Record(schema)
      record.put("doubles", listOf(12.54, 23.5, 9123.2314))
      Avro.default.fromRecord(TestListDoubles.serializer(), record) shouldBe
          TestListDoubles(listOf(12.54, 23.5, 9123.2314))
    }

    "support GenericData.Array for a List of doubles" {

      val schema = Avro.default.schema(TestListDoubles.serializer())
      val record = GenericData.Record(schema)
      record.put("doubles",
          GenericData.Array(Schema.createArray(Schema.create(Schema.Type.DOUBLE)), listOf(12.54, 23.5, 9123.2314)))
      Avro.default.fromRecord(TestListDoubles.serializer(), record) shouldBe
          TestListDoubles(listOf(12.54, 23.5, 9123.2314))
    }

    "support array for a List of records" {

      val containerSchema = Avro.default.schema(TestListRecords.serializer())
      val recordSchema = Avro.default.schema(Record.serializer())

      val record1 = GenericData.Record(recordSchema)
      record1.put("str", "qwe")
      record1.put("double", 123.4)

      val record2 = GenericData.Record(recordSchema)
      record2.put("str", "wer")
      record2.put("double", 8234.324)

      val container = GenericData.Record(containerSchema)
      container.put("records", arrayOf(record1, record2))

      Avro.default.fromRecord(TestListRecords.serializer(), container) shouldBe
          TestListRecords(listOf(Record("qwe", 123.4), Record("wer", 8234.324)))
    }

    "support list for a List of records" {

      val containerSchema = Avro.default.schema(TestListRecords.serializer())
      val recordSchema = Avro.default.schema(Record.serializer())

      val record1 = GenericData.Record(recordSchema)
      record1.put("str", "qwe")
      record1.put("double", 123.4)

      val record2 = GenericData.Record(recordSchema)
      record2.put("str", "wer")
      record2.put("double", 8234.324)

      val container = GenericData.Record(containerSchema)
      container.put("records", listOf(record1, record2))

      Avro.default.fromRecord(TestListRecords.serializer(), container) shouldBe
          TestListRecords(listOf(Record("qwe", 123.4), Record("wer", 8234.324)))
    }

    "support array for a Set of records" {

      val containerSchema = Avro.default.schema(TestSetRecords.serializer())
      val recordSchema = Avro.default.schema(Record.serializer())

      val record1 = GenericData.Record(recordSchema)
      record1.put("str", "qwe")
      record1.put("double", 123.4)

      val record2 = GenericData.Record(recordSchema)
      record2.put("str", "wer")
      record2.put("double", 8234.324)

      val container = GenericData.Record(containerSchema)
      container.put("records", arrayOf(record1, record2))

      Avro.default.fromRecord(TestSetRecords.serializer(), container) shouldBe
          TestSetRecords(setOf(Record("qwe", 123.4), Record("wer", 8234.324)))
    }

    "support GenericData.Array for a Set of records" {

      val containerSchema = Avro.default.schema(TestSetRecords.serializer())
      val recordSchema = Avro.default.schema(Record.serializer())

      val record1 = GenericData.Record(recordSchema)
      record1.put("str", "qwe")
      record1.put("double", 123.4)

      val record2 = GenericData.Record(recordSchema)
      record2.put("str", "wer")
      record2.put("double", 8234.324)

      val container = GenericData.Record(containerSchema)
      container.put("records",
          GenericData.Array(Schema.createArray(Schema.create(Schema.Type.STRING)), listOf(record1, record2)))

      Avro.default.fromRecord(TestSetRecords.serializer(), container) shouldBe
          TestSetRecords(setOf(Record("qwe", 123.4), Record("wer", 8234.324)))
    }

    "support array for a Set of strings" {
      val schema = Avro.default.schema(TestSetString.serializer())
      val record = GenericData.Record(schema)
      record.put("strings", arrayOf("Qwe", "324", "q"))
      Avro.default.fromRecord(TestSetString.serializer(), record) shouldBe
          TestSetString(setOf("Qwe", "324", "q"))
    }

    "support list for a Set of strings" {
      val schema = Avro.default.schema(TestSetString.serializer())
      val record = GenericData.Record(schema)
      record.put("strings", arrayOf("Qwe", "324", "q"))
      Avro.default.fromRecord(TestSetString.serializer(), record) shouldBe
          TestSetString(setOf("Qwe", "324", "q"))
    }

    "support GenericData.Array for a Set of strings" {
      val schema = Avro.default.schema(TestSetString.serializer())
      val record = GenericData.Record(schema)
      record.put("strings",
          GenericData.Array(Schema.createArray(Schema.create(Schema.Type.STRING)), listOf("Qwe", "324", "q")))
      Avro.default.fromRecord(TestSetString.serializer(), record) shouldBe
          TestSetString(setOf("Qwe", "324", "q"))
    }
  }

})