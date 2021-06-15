@file:UseSerializers(URLSerializer::class)

package com.github.avrokotlin.avro4k.decoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.serializer.URLSerializer
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import java.net.URL

class URLDecoderTest : FunSpec({

   test("decode String to URL") {

      val schema = Avro.default.schema(TestUrl.serializer())

      val record = GenericData.Record(schema)
      record.put("b", "http://www.sksamuel.com")

      Avro.default.fromRecord(TestUrl.serializer(), record) shouldBe TestUrl(URL("http://www.sksamuel.com"))
   }

   test("decode UT8 to URL") {

      val schema = Avro.default.schema(TestUrl.serializer())

      val record = GenericData.Record(schema)
      record.put("b", Utf8("http://www.sksamuel.com"))
      Avro.default.fromRecord(TestUrl.serializer(), record) shouldBe TestUrl(URL("http://www.sksamuel.com"))
   }

   test("decode list of Strings to URL") {

      val schema = Avro.default.schema(TestUrlList.serializer())

      val record = GenericData.Record(schema)
      record.put("urls", listOf("http://www.sksamuel.com", "https://sksamuel.com"))
      Avro.default.fromRecord(TestUrlList.serializer(), record) shouldBe TestUrlList(listOf(URL("http://www.sksamuel.com"), URL("https://sksamuel.com")))

   }

   test("decode list of UT8s to URL") {

      val schema = Avro.default.schema(TestUrlList.serializer())

      val record = GenericData.Record(schema)
      record.put("urls", listOf(Utf8("http://www.sksamuel.com"), Utf8("https://sksamuel.com")))
      Avro.default.fromRecord(TestUrlList.serializer(), record) shouldBe TestUrlList(listOf(URL("http://www.sksamuel.com"), URL("https://sksamuel.com")))

   }
}) {

   @Serializable
   data class TestUrl(val b: URL)

   @Serializable
   data class TestUrlList(val urls: List<URL>)
}
