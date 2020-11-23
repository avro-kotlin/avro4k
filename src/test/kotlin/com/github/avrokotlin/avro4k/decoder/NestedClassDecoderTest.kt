package com.github.avrokotlin.avro4k.decoder

import com.github.avrokotlin.avro4k.Avro
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.WordSpec
import kotlinx.serialization.Serializable
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8

// data class OptionCounty(county: Option[County])

@Serializable
data class County(val name: String, val towns: List<Town>, val ceremonial: Boolean, val lat: Double, val long: Double)

@Serializable
data class Town(val name: String, val population: Int)

@Serializable
data class Birthplace(val person: String, val town: Town)

class NestedClassDecoderTest : WordSpec({

  "Decoder" should {

    "decode nested class" {

      val townSchema = Avro.default.schema(Town.serializer())
      val birthplaceSchema = Avro.default.schema(Birthplace.serializer())

      val hardwick = GenericData.Record(townSchema)
      hardwick.put("name", Utf8("Hardwick"))
      hardwick.put("population", 123)

      val birthplace = GenericData.Record(birthplaceSchema)
      birthplace.put("person", Utf8("Sammy Sam"))
      birthplace.put("town", hardwick)

      Avro.default.fromRecord(Birthplace.serializer(), birthplace) shouldBe
          Birthplace(person = "Sammy Sam", town = Town(name = "Hardwick", population = 123))
    }

    "decode nested list of classes" {

      val countySchema = Avro.default.schema(County.serializer())
      val townSchema = Avro.default.schema(Town.serializer())

      val hardwick = GenericData.Record(townSchema)
      hardwick.put("name", "Hardwick")
      hardwick.put("population", 123)

      val weedon = GenericData.Record(townSchema)
      weedon.put("name", "Weedon")
      weedon.put("population", 225)

      val bucks = GenericData.Record(countySchema)
      bucks.put("name", "Bucks")
      bucks.put("towns", listOf(hardwick, weedon))
      bucks.put("ceremonial", true)
      bucks.put("lat", 12.34)
      bucks.put("long", 0.123)

      Avro.default.fromRecord(County.serializer(), bucks) shouldBe
          County(
              name = "Bucks",
              towns = listOf(Town(name = "Hardwick", population = 123), Town(name = "Weedon", population = 225)),
              ceremonial = true,
              lat = 12.34,
              long = 0.123
          )
    }

//    "decode optional structs" {
//      val countySchema = AvroSchema[County]
//      val townSchema = AvroSchema[Town]
//      val optionCountySchema = AvroSchema[OptionCounty]
//
//      val obj = OptionCounty(Some(County("Bucks", Seq(Town("Hardwick", 123), Town("Weedon", 225)), true, 12.34, 0.123)))
//
//      val hardwick = new GenericData.Record(townSchema)
//      hardwick.put("name", "Hardwick")
//      hardwick.put("population", 123)
//
//      val weedon = new GenericData.Record(townSchema)
//      weedon.put("name", "Weedon")
//      weedon.put("population", 225)
//
//      val bucks = new GenericData.Record(countySchema)
//      bucks.put("name", "Bucks")
//      bucks.put("towns", List(hardwick, weedon).asJava)
//      bucks.put("ceremonial", true)
//      bucks.put("lat", 12.34)
//      bucks.put("long", 0.123)
//
//      val record = new GenericData.Record(optionCountySchema)
//      record.put("county", bucks)
//
//      Decoder[OptionCounty].decode(record, optionCountySchema, DefaultFieldMapper) shouldBe obj
//
//      val emptyRecord = new GenericData.Record(optionCountySchema)
//      emptyRecord.put("county", null)
//
//      Decoder[OptionCounty].decode(emptyRecord, optionCountySchema, DefaultFieldMapper) shouldBe OptionCounty(None)
//    }
  }

})
