@file:UseSerializers(BigIntegerSerializer::class)

package com.github.avrokotlin.avro4k.decoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.serializer.BigIntegerSerializer
import io.kotest.matchers.shouldBe
import io.kotest.core.spec.style.FunSpec
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import java.math.BigInteger

class BigIntegerDecoderTest : FunSpec({

   test("decode big integer from string") {

      val schema = SchemaBuilder.record("Test").fields().name("b").type(Schema.create(Schema.Type.STRING)).noDefault().endRecord()

      val record = GenericData.Record(schema)
      record.put("b", Utf8("1927398217318546456532973912379127391279312983719"))

      Avro.default.fromRecord(Test.serializer(), record) shouldBe Test(BigInteger("1927398217318546456532973912379127391279312983719"))
   }
}) {

   @Serializable
   data class Test(val b: BigInteger)
}
