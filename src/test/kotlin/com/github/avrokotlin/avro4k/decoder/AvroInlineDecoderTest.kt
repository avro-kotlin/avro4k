package com.github.avrokotlin.avro4k.decoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroInline
import com.github.avrokotlin.avro4k.ListRecord
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.Serializable
import org.apache.avro.util.Utf8

class AvroInlineDecoderTest : FunSpec({

   test("decode @AvroInline") {

      val schema = Avro.default.schema(Product.serializer())
      val record =  ListRecord(schema, listOf(Utf8("123"), Utf8("sneakers")))
      Avro.default.fromRecord(Product.serializer(), record) shouldBe Product("123", Name("sneakers"))
   }

}) {

   @Serializable
   @AvroInline
   data class Name(val value: String)

   @Serializable
   data class Product(val id: String, val name: Name)
}
