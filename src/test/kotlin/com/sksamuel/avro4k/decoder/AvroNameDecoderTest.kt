package com.sksamuel.avro4k.decoder

import com.sksamuel.avro4k.Avro
import com.sksamuel.avro4k.AvroName
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.Serializable
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8

class AvroNameDecoderTest : FunSpec({

   @Serializable
   data class Foo(@AvroName("bar") val foo: String)

   test("decoder should take into account @AvroName on fields") {
      val schema = Avro.default.schema(Foo.serializer())
      val record = GenericData.Record(schema)
      record.put("bar", Utf8("hello"))
      Avro.default.fromRecord(Foo.serializer(), record) shouldBe Foo("hello")
   }
})