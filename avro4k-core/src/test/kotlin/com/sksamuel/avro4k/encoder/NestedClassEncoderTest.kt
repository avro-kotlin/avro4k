@file:UseSerializers(
   TimestampSerializer::class
)

package com.sksamuel.avro4k.encoder

import com.sksamuel.avro4k.Avro
import com.sksamuel.avro4k.ListRecord
import com.sksamuel.avro4k.serializer.TimestampSerializer
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import org.apache.avro.util.Utf8
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDateTime

@Serializable
data class County(val name: String, val towns: List<Town>, val ceremonial: Boolean, val lat: Double, val long: Double)

@Serializable
data class Town(val name: String, val population: Int)

@Serializable
data class Birthplace(val name: String, val town: Town)

@Serializable
data class ProductName(val value: String)

@Serializable
data class Product(val name: ProductName?)

class NestedClassEncoderTest : FunSpec({

   val townSchema = Avro.default.schema(Town.serializer())
   val birthplaceSchema = Avro.default.schema(Birthplace.serializer())

   test("!encode nested class") {
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

   test("!encode nested nullable class") {

      val nameSchema = Avro.default.schema(ProductName.serializer())
      val prodSchema = Avro.default.schema(Product.serializer())

      val p = Product(ProductName("big shoes"))
      val record = Avro.default.toRecord(Product.serializer(), p)
      record shouldBe ListRecord(
         prodSchema,
         ListRecord(
            nameSchema,
            Utf8("big shoes")
         )
      )
   }
})