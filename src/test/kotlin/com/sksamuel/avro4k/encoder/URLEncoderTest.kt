@file:UseSerializers(URLSerializer::class)

package com.sksamuel.avro4k.encoder

import com.sksamuel.avro4k.Avro
import com.sksamuel.avro4k.ListRecord
import com.sksamuel.avro4k.serializer.URLSerializer
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import org.apache.avro.util.Utf8
import java.net.URL

class URLEncoderTest : FunSpec({

   test("use string for URL") {

      @Serializable
      data class Test(val b: URL)

      val schema = Avro.default.schema(Test.serializer())

      val test = Test(URL("http://www.sksamuel.com"))

      Avro.default.toRecord(Test.serializer(), schema, test) shouldBe ListRecord(schema, Utf8("http://www.sksamuel.com"))
   }

   test("encode nullable URLs") {

      @Serializable
      data class Test(val b: URL?)

      val schema = Avro.default.schema(Test.serializer())
      Avro.default.toRecord(Test.serializer(), schema, Test(URL("http://www.sksamuel.com"))) shouldBe
         ListRecord(schema, Utf8("http://www.sksamuel.com"))
      Avro.default.toRecord(Test.serializer(), schema, Test(null)) shouldBe ListRecord(schema, null)

   }
})