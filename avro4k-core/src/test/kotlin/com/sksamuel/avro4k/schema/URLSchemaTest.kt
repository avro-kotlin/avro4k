@file:UseSerializers(URLSerializer::class)

package com.sksamuel.avro4k.schema

import com.sksamuel.avro4k.Avro
import com.sksamuel.avro4k.serializer.URLSerializer
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import java.net.URL

class URLSchemaTest : FunSpec({

   test("accept URL as String") {

      @Serializable
      data class Test(val b: URL)

      val schema = Avro.default.schema(Test.serializer())
      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/url.json"))
      schema shouldBe expected
   }

   test("accept nullable URL as String union") {

      @Serializable
      data class Test(val b: URL?)

      val schema = Avro.default.schema(Test.serializer())
      val expected = org.apache.avro.Schema.Parser().parse(javaClass.getResourceAsStream("/url_nullable.json"))
      schema shouldBe expected
   }
})