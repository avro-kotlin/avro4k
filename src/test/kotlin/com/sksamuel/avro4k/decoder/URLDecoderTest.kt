@file:UseSerializers(URLSerializer::class)

package com.sksamuel.avro4k.decoder

import com.sksamuel.avro4k.Avro
import com.sksamuel.avro4k.serializer.URLSerializer
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import java.net.URL

class URLDecoderTest : FunSpec({

   test("decode String to URL") {

      @Serializable
      data class Test(val b: URL)

      val schema = Avro.default.schema(Test.serializer())

      val record = GenericData.Record(schema)
      record.put("b", "http://www.sksamuel.com")

      Avro.default.fromRecord(Test.serializer(), record) shouldBe Test(URL("http://www.sksamuel.com"))
   }

   test("decode UT8 to URL") {

      @Serializable
      data class Test(val b: URL)

      val schema = Avro.default.schema(Test.serializer())

      val record = GenericData.Record(schema)
      record.put("b", Utf8("http://www.sksamuel.com"))
      Avro.default.fromRecord(Test.serializer(), record) shouldBe Test(URL("http://www.sksamuel.com"))
   }

   test("decode list of Strings to URL") {

      @Serializable
      data class Test(val urls: List<URL>)

      val schema = Avro.default.schema(Test.serializer())

      val record = GenericData.Record(schema)
      record.put("urls", listOf("http://www.sksamuel.com", "https://sksamuel.com"))
      Avro.default.fromRecord(Test.serializer(), record) shouldBe Test(listOf(URL("http://www.sksamuel.com"), URL("https://sksamuel.com")))

   }

   test("decode list of UT8s to URL") {

      @Serializable
      data class Test(val urls: List<URL>)

      val schema = Avro.default.schema(Test.serializer())

      val record = GenericData.Record(schema)
      record.put("urls", listOf(Utf8("http://www.sksamuel.com"), Utf8("https://sksamuel.com")))
      Avro.default.fromRecord(Test.serializer(), record) shouldBe Test(listOf(URL("http://www.sksamuel.com"), URL("https://sksamuel.com")))

   }
})