package com.sksamuel.avro4k.encoder

import com.sksamuel.avro4k.Avro
import com.sksamuel.avro4k.ListRecord
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import kotlinx.serialization.Serializable
import org.apache.avro.util.Utf8

@Serializable
data class County(val name: String, val towns: List<Town>, val ceremonial: Boolean, val lat: Double, val long: Double)

@Serializable
data class Town(val name: String, val population: Int)

@Serializable
data class Birthplace(val name: String, val town: Town)

class NestedClassEncoderTest : FunSpec({

   val townSchema = Avro.default.schema(Town.serializer())
   val birthplaceSchema = Avro.default.schema(Birthplace.serializer())

   test("encode nested class") {
      val b = Birthplace("sammy", Town("Hardwick", 123))
      val record = Avro.default.toRecord(Birthplace.serializer(), b)
      record shouldBe ListRecord(
         birthplaceSchema,
         Utf8("sammy"),
         ListRecord(
            townSchema,
            Utf8("Hardwick"),
            123
         )
      )
   }
})
