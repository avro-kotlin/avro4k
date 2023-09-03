@file:UseSerializers(URLSerializer::class)

package com.github.avrokotlin.avro4k.encoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.ListRecord
import com.github.avrokotlin.avro4k.encode
import com.github.avrokotlin.avro4k.serializer.URLSerializer
import com.github.avrokotlin.avro4k.shouldBeContentOf
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import org.apache.avro.util.Utf8
import java.net.URL

class URLEncoderTest : FunSpec({

   test("use string for URL") {

      val schema = Avro.default.schema(UrlTest.serializer())

      val test = UrlTest(URL("http://www.sksamuel.com"))

       Avro.default.encode(schema, test) shouldBeContentOf ListRecord(schema, Utf8("http://www.sksamuel.com"))
   }

   test("encode nullable URLs") {

      val schema = Avro.default.schema(NullableUrlTest.serializer())
       Avro.default.encode(schema, NullableUrlTest(URL("http://www.sksamuel.com"))) shouldBeContentOf
         ListRecord(schema, Utf8("http://www.sksamuel.com"))
       Avro.default.encode(schema, NullableUrlTest(null)) shouldBeContentOf ListRecord(schema, null)

   }
}) {
   @Serializable
   data class UrlTest(val b: URL)

   @Serializable
   data class NullableUrlTest(val b: URL?)
}
