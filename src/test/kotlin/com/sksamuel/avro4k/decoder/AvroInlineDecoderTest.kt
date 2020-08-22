package com.sksamuel.avro4k.decoder

import com.sksamuel.avro4k.Avro
import com.sksamuel.avro4k.AvroInline
import com.sksamuel.avro4k.ListRecord
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import kotlinx.serialization.Serializable
import org.apache.avro.util.Utf8

class AvroInlineDecoderTest : FunSpec({

   test("decode @AvroInline") {

      @Serializable
      @AvroInline
      data class Name(val value: String)

      @Serializable
      data class Product(val id: String, val name: Name)

      val schema = Avro.default.schema(Product.serializer())
      val record =  ListRecord(schema, listOf(Utf8("123"), Utf8("sneakers")))
      Avro.default.fromRecord(Product.serializer(), record) shouldBe Product("123", Name("sneakers"))
   }

})