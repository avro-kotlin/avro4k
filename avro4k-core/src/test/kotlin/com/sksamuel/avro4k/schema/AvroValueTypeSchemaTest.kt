@file:UseSerializers(UUIDSerializer::class)

package com.sksamuel.avro4k.schema

import com.sksamuel.avro4k.Avro
import com.sksamuel.avro4k.AvroValueType
import com.sksamuel.avro4k.serializer.UUIDSerializer
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers

class AvroValueTypeSchemaTest : FunSpec({

   test("support @AvroValueType") {

      @Serializable
      @AvroValueType
      data class Name(val value: String)

      @Serializable
      data class Product(val id: String, val name: Name)

      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/value_type.json"))
      val schema = Avro.default.schema(Product.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }

})