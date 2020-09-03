package com.sksamuel.avro4k.decoder

import com.sksamuel.avro4k.Avro
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import org.apache.avro.generic.GenericData

class TransientDecoderTest : FunSpec({

   test("decoder should populate transient fields with default") {

      @Serializable
      data class TransientFoo(val a: String, @Transient val b: String = "world")

      val schema = Avro.default.schema(TransientFoo.serializer())
      val record = GenericData.Record(schema)
      record.put("a", "hello")
      Avro.default.fromRecord(TransientFoo.serializer(), record) shouldBe TransientFoo("hello", "world")
   }
})