@file:UseSerializers(
   TimestampSerializer::class
)

package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.ListRecord
import com.github.avrokotlin.avro4k.encodeToGenericData
import com.github.avrokotlin.avro4k.serializer.TimestampSerializer
import com.github.avrokotlin.avro4k.shouldBeContentOf
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import org.apache.avro.util.Utf8

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

   test("encode nested class") {
      val b = Birthplace("sammy", Town("Hardwick", 123))
      val record = Avro.default.encodeToGenericData(b, Birthplace.serializer())
      record shouldBeContentOf ListRecord(
              birthplaceSchema,
              Utf8("sammy"),
              ListRecord(
                      townSchema,
                      Utf8("Hardwick"),
                      123
              )
      )
   }

   test("encode nested nullable class") {

      val nameSchema = Avro.default.schema(ProductName.serializer())
      val prodSchema = Avro.default.schema(Product.serializer())

      val p = Product(ProductName("big shoes"))
      val record = Avro.default.encodeToGenericData(p, Product.serializer())
      record shouldBeContentOf ListRecord(
              prodSchema,
              ListRecord(
                      nameSchema,
                      Utf8("big shoes")
              )
      )
   }
})