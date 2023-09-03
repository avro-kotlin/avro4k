package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.Avro
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable

class AvroInlineSchemaTest : FunSpec({

   test("support @AvroInline") {

      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/value_type.json"))
      val schema = Avro.default.schema(Product.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }

}) {

   @JvmInline
   @Serializable
   value class Name(val value: String)

   @Serializable
   data class Product(val id: String, val name: Name)
}
