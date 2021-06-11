package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.Avro
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable

class ValueClassSchemaTest : StringSpec({

   "value class should be primitive in schema" {
      val expected =
         org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/value_class.json"))
      val schema = Avro.default.schema(ContainsInlineTest.serializer())
      schema.toString(true) shouldBe expected.toString(true)
   }
}) {
   @Serializable
   @JvmInline
   value class StringWrapper(val a: String)

   @Serializable
   data class ContainsInlineTest(val id: StringWrapper)
}