package com.sksamuel.avro4k.encoder

import com.sksamuel.avro4k.Avro
import com.sksamuel.avro4k.ImmutableRecord
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import kotlinx.serialization.Serializable
import org.apache.avro.util.Utf8

@Serializable
data class County(val name: String, val towns: List<Town>, val ceremonial: Boolean, val lat: Double, val long: Double)

@Serializable
data class Town(val name: String, val population: Int)

@Serializable
data class Birthplace(val a: String, val town: Town)

class NestedClassEncoderTest : FunSpec({

  val countySchema = Avro.default.schema(County.serializer())
  val townSchema = Avro.default.schema(Town.serializer())
  val hardwick = ImmutableRecord(townSchema, arrayListOf(Utf8("Hardwick"), Integer.valueOf(123)))
  val weedon = ImmutableRecord(townSchema, arrayListOf(Utf8("Weedon"), Integer.valueOf(225)))

  test("encode nested class") {
    val foo = Birthplace("wibble", Town("Hardwick", 123))
    val record = Avro.default.toRecord(Birthplace.serializer(), foo)
    record shouldBe ImmutableRecord(countySchema, arrayListOf(Utf8("wibble"), hardwick))
  }

  test("encode lists of nested classes") {

    val county = County("Bucks", listOf(Town("Hardwick", 123), Town("Weedon", 225)), true, 12.34, 0.123)
    val record = Avro.default.toRecord(County.serializer(), county)
    record shouldBe ImmutableRecord(countySchema, arrayListOf(true))

    record shouldBe ImmutableRecord(
        countySchema,
        arrayListOf(
            Utf8("Bucks"),
            listOf(hardwick, weedon),
            true,
            12.34,
            0.123)
    )
  }
})
