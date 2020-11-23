package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroInline
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.Serializable

class AvroInlineSchemaTest : FunSpec({

   test("support @AvroInline") {

      @Serializable
      @AvroInline
      data class Name(val value: String)

      @Serializable
      data class Product(val id: String, val name: Name)

      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/value_type.json"))
      val schema = Avro.default.schema(Product.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }

})